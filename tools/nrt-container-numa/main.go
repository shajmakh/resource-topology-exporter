// nrt-container-numa resolves a workload container's NUMA node from the
// NodeResourceTopology (NRT) custom resource for the node the pod runs on.
//
// It requires NRT objects published by resource-topology-exporter with
// numaplacement attributes (enabled when podSetFingerprint is true and
// topology manager policy is single-numa-node, as in default RTE configs).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	topologyv1alpha2 "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/apis/topology/v1alpha2"
	topologyclientset "github.com/k8stopologyawareschedwg/noderesourcetopology-api/pkg/generated/clientset/versioned"
	"github.com/k8stopologyawareschedwg/numaplacement"
	"github.com/k8stopologyawareschedwg/numaplacement/plainmap"

	"github.com/k8stopologyawareschedwg/resource-topology-exporter/pkg/k8shelpers"
)

func main() {
	kubeconfig := flag.String("kubeconfig", "", "path to kubeconfig (empty for in-cluster or $KUBECONFIG)")
	containerID := flag.String("container-id", "", "container ID as in Pod status (e.g. containerd://...) or bare runtime ID")
	maxCombinations := flag.Uint64("max-combinations", 100000, "max subset trials when reconciling leb89 placement with on-node pods (0 disables limit)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s -container-id=<runtime-id>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Looks up the pod by container ID, loads NodeResourceTopology for the pod's node,\n")
		fmt.Fprintf(os.Stderr, "decodes numaplacement data, and prints the NUMA node index for that container.\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	if strings.TrimSpace(*containerID) == "" {
		fmt.Fprintln(os.Stderr, "error: -container-id is required")
		flag.Usage()
		os.Exit(2)
	}

	ctx := context.Background()

	kube, err := k8shelpers.GetK8sClient(*kubeconfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "kubernetes client: %v\n", err)
		os.Exit(1)
	}
	topo, err := k8shelpers.GetTopologyClient(*kubeconfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "topology client: %v\n", err)
		os.Exit(1)
	}

	ns, pod, cname, node, err := findPodByContainerID(ctx, kube, *containerID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	pl, err := payloadFromNRT(ctx, topo, node)
	if err != nil {
		fmt.Fprintf(os.Stderr, "NRT for node %q: %v\n", node, err)
		os.Exit(1)
	}

	if pl.Containers == 0 {
		fmt.Fprintf(os.Stderr, "NRT on node %q has no numaplacement data (containers=0). Check RTE podSetFingerprint and kubelet topologyManagerPolicy=single-numa-node.\n", node)
		os.Exit(1)
	}

	info, err := resolvePlacement(ctx, kube, node, pl, ns, pod, cname, *maxCombinations)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	numa, err := info.NUMAAffinityContainer(ns, pod, cname)
	if err != nil {
		fmt.Fprintf(os.Stderr, "lookup %s/%s/%s in decoded placement: %v\n", ns, pod, cname, err)
		os.Exit(1)
	}

	fmt.Println(numa)
}

func findPodByContainerID(ctx context.Context, kube kubernetes.Interface, want string) (namespace, podName, containerName, node string, err error) {
	wantNorm := normalizeRuntimeID(want)
	var token string
	if strings.Contains(wantNorm, ":") {
		parts := strings.SplitN(wantNorm, ":", 2)
		token = parts[len(parts)-1]
	} else {
		token = wantNorm
	}

	match := func(raw string) bool {
		if raw == "" {
			return false
		}
		rawNorm := normalizeRuntimeID(raw)
		if rawNorm == wantNorm {
			return true
		}
		if token != "" && (rawNorm == token || strings.HasSuffix(rawNorm, token) || strings.HasSuffix(token, rawNorm)) {
			return true
		}
		return false
	}

	listOpts := metav1.ListOptions{Limit: 200}
	for {
		list, lerr := kube.CoreV1().Pods("").List(ctx, listOpts)
		if lerr != nil {
			return "", "", "", "", fmt.Errorf("list pods: %w", lerr)
		}
		for i := range list.Items {
			p := &list.Items[i]
			if p.Spec.NodeName == "" {
				continue
			}
			for _, cs := range p.Status.ContainerStatuses {
				if match(cs.ContainerID) {
					return p.Namespace, p.Name, cs.Name, p.Spec.NodeName, nil
				}
			}
			for _, cs := range p.Status.InitContainerStatuses {
				if match(cs.ContainerID) {
					return p.Namespace, p.Name, cs.Name, p.Spec.NodeName, nil
				}
			}
		}
		if list.Continue == "" {
			break
		}
		listOpts.Continue = list.Continue
	}

	return "", "", "", "", fmt.Errorf("no pod found with containerID matching %q", want)
}

func normalizeRuntimeID(id string) string {
	id = strings.TrimSpace(id)
	lower := strings.ToLower(id)
	for _, p := range []string{"containerd://", "cri-o://", "docker://", "podman://"} {
		if strings.HasPrefix(lower, p) {
			return id[len(p):]
		}
	}
	return id
}

func attrValue(attrs topologyv1alpha2.AttributeList, name string) (string, bool) {
	for i := range attrs {
		if attrs[i].Name == name {
			return attrs[i].Value, true
		}
	}
	return "", false
}

func payloadFromNRT(ctx context.Context, topo *topologyclientset.Clientset, nodeName string) (numaplacement.Payload, error) {
	nrt, err := topo.TopologyV1alpha2().NodeResourceTopologies().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return numaplacement.Payload{}, err
	}

	meta, ok := attrValue(nrt.Attributes, numaplacement.AttributeMetadata)
	if !ok || meta == "" {
		return numaplacement.Payload{}, fmt.Errorf("missing attribute %q", numaplacement.AttributeMetadata)
	}

	var pl numaplacement.Payload
	if err := numaplacement.UnpackMetadataInto(&pl, meta); err != nil {
		return numaplacement.Payload{}, fmt.Errorf("unpack numaplacement metadata: %w", err)
	}

	pl.Vectors = make(map[int]string)
	prefix := numaplacement.Prefix + numaplacement.Version
	for _, z := range nrt.Zones {
		numaID, ok := parseNumaZoneName(z.Name)
		if !ok {
			continue
		}
		vec, ok := attrValue(z.Attributes, numaplacement.AttributeVector)
		if !ok || vec == "" {
			continue
		}
		if !strings.HasPrefix(vec, prefix) {
			return numaplacement.Payload{}, fmt.Errorf("zone %q: vector missing expected prefix", z.Name)
		}
		pl.Vectors[numaID] = strings.TrimPrefix(vec, prefix)
	}
	return pl, nil
}

func parseNumaZoneName(name string) (int, bool) {
	const pfx = "node-"
	if !strings.HasPrefix(name, pfx) {
		return 0, false
	}
	id, err := strconv.Atoi(strings.TrimPrefix(name, pfx))
	if err != nil || id < 0 {
		return 0, false
	}
	return id, true
}

func runningContainerIDsOnNode(ctx context.Context, kube kubernetes.Interface, nodeName string) ([]numaplacement.ContainerID, error) {
	sel := fmt.Sprintf("spec.nodeName=%s,status.phase=Running", nodeName)
	var out []numaplacement.ContainerID
	listOpts := metav1.ListOptions{FieldSelector: sel, Limit: 200}
	for {
		list, err := kube.CoreV1().Pods("").List(ctx, listOpts)
		if err != nil {
			return nil, err
		}
		for i := range list.Items {
			p := &list.Items[i]
			for _, cs := range p.Status.ContainerStatuses {
				if cs.State.Running == nil {
					continue
				}
				out = append(out, numaplacement.ContainerID{
					Namespace:     p.Namespace,
					PodName:       p.Name,
					ContainerName: cs.Name,
				})
			}
		}
		if list.Continue == "" {
			break
		}
		listOpts.Continue = list.Continue
	}
	return out, nil
}

func containerIDsFromPlainVectors(pl numaplacement.Payload) ([]numaplacement.ContainerID, error) {
	if pl.VectorEncoding != plainmap.VectorEncodingPlain {
		return nil, fmt.Errorf("not plain vector encoding")
	}
	seen := make(map[string]struct{})
	var ids []numaplacement.ContainerID
	for _, wire := range pl.Vectors {
		rest := wire
		for rest != "" {
			triple, after, found := strings.Cut(rest, "|")
			if triple == "" {
				return nil, fmt.Errorf("malformed plain placement vector")
			}
			if _, dup := seen[triple]; dup {
				return nil, fmt.Errorf("duplicate triple %q in plain vector", triple)
			}
			seen[triple] = struct{}{}
			id, err := parseContainerTriple(triple)
			if err != nil {
				return nil, err
			}
			ids = append(ids, id)
			if !found {
				break
			}
			rest = after
		}
	}
	if len(ids) != pl.Containers {
		return nil, fmt.Errorf("plain vector container count %d != metadata cc=%d", len(ids), pl.Containers)
	}
	return ids, nil
}

func parseContainerTriple(s string) (numaplacement.ContainerID, error) {
	parts := strings.Split(s, "/")
	if len(parts) != 3 {
		return numaplacement.ContainerID{}, fmt.Errorf("expected namespace/pod/container, got %q", s)
	}
	return numaplacement.ContainerID{
		Namespace:     parts[0],
		PodName:       parts[1],
		ContainerName: parts[2],
	}, nil
}

func resolvePlacement(ctx context.Context, kube kubernetes.Interface, node string, pl numaplacement.Payload,
	targetNS, targetPod, targetContainer string, maxComb uint64) (numaplacement.Info, error) {

	if pl.VectorEncoding == plainmap.VectorEncodingPlain {
		ids, err := containerIDsFromPlainVectors(pl)
		if err != nil {
			return nil, fmt.Errorf("plain numaplacement: %w", err)
		}
		dec, err := plainmap.NewDecoder(pl, ids...)
		if err != nil {
			return nil, err
		}
		return dec.Result()
	}

	if pl.VectorEncoding != numaplacement.VectorEncodingLEB89 {
		return nil, fmt.Errorf("unsupported vector encoding %q", pl.VectorEncoding)
	}

	candidates, err := runningContainerIDsOnNode(ctx, kube, node)
	if err != nil {
		return nil, fmt.Errorf("list pods on node %q: %w", node, err)
	}

	if len(candidates) == pl.Containers {
		return decodeLEB89(pl, candidates)
	}

	if len(candidates) < pl.Containers {
		return nil, fmt.Errorf("NRT encodes %d containers but found %d running containers on node %q (NRT may be ahead of the API or workloads changed)", pl.Containers, len(candidates), node)
	}

	// Subset search: RTE encodes a subset of running containers (PodResources + locality filters).
	k := pl.Containers
	n := len(candidates)
	combCount := nChooseK(n, k)
	if maxComb > 0 && combCount > maxComb {
		return nil, fmt.Errorf("too many combinations C(%d,%d)=%d to try (limit %d); narrow workloads or increase -max-combinations",
			n, k, combCount, maxComb)
	}

	var lastDecErr error
	var matched numaplacement.Info
	found := false
	forEachCombination(n, k, func(idxs []int) bool {
		sub := make([]numaplacement.ContainerID, k)
		for i, ix := range idxs {
			sub[i] = candidates[ix]
		}
		info, err := decodeLEB89(pl, sub)
		if err != nil {
			lastDecErr = err
			return true
		}
		if _, err := info.NUMAAffinityContainer(targetNS, targetPod, targetContainer); err == nil {
			matched = info
			found = true
			return false
		}
		return true
	})
	if found {
		return matched, nil
	}
	if lastDecErr != nil {
		return nil, fmt.Errorf("could not decode numaplacement for node %q (last decode error): %w", node, lastDecErr)
	}
	return nil, fmt.Errorf("no leb89 subset of %d on-node containers matched NRT metadata (target %s/%s/%s)", n, targetNS, targetPod, targetContainer)
}

func decodeLEB89(pl numaplacement.Payload, ids []numaplacement.ContainerID) (numaplacement.Info, error) {
	dec, err := numaplacement.NewDecoder(pl, ids...)
	if err != nil {
		return nil, err
	}
	return dec.Result()
}

// forEachCombination invokes fn for each k-combination of indices in [0,n).
// If fn returns false, iteration stops early.
func forEachCombination(n, k int, fn func([]int) bool) {
	if k < 0 || k > n {
		return
	}
	idx := make([]int, k)
	var build func(pos, start int) bool
	build = func(pos, start int) bool {
		if pos == k {
			return fn(append([]int(nil), idx...))
		}
		end := n - (k - pos - 1)
		for i := start; i < end; i++ {
			idx[pos] = i
			if !build(pos+1, i+1) {
				return false
			}
		}
		return true
	}
	build(0, 0)
}

func nChooseK(n, k int) uint64 {
	if k < 0 || k > n {
		return 0
	}
	if k > n-k {
		k = n - k
	}
	var c uint64 = 1
	for i := 0; i < k; i++ {
		c = c * uint64(n-i) / uint64(i+1)
	}
	return c
}
