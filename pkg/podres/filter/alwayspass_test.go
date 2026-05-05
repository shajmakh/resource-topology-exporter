package filter

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/testing/protocmp"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"
)

func TestAlwaysPass(t *testing.T) {
	type testCase struct {
		name string
		pr   *podresourcesapi.PodResources
	}

	testCases := []testCase{
		{
			name: "nil reference",
		},
		{
			name: "no exclusive resources",
			pr: &podresourcesapi.PodResources{
				Name:      "image-registry-78b84dc9f9-zwxtk",
				Namespace: "image-registry",
				Containers: []*podresourcesapi.ContainerResources{
					{
						Name: "registry",
					},
				},
			},
		},
		{
			name: "exclusive CPUs",
			pr: &podresourcesapi.PodResources{
				Name:      "highperf-cpus",
				Namespace: "exclusive-resources",
				Containers: []*podresourcesapi.ContainerResources{
					{
						Name:   "compute-intensive",
						CpuIds: []int64{0, 2, 4, 6},
					},
				},
			},
		},
		{
			name: "have devices no topology",
			pr: &podresourcesapi.PodResources{
				Name:      "highperf-devs-no-topology",
				Namespace: "exclusive-resources",
				Containers: []*podresourcesapi.ContainerResources{
					{
						Name: "require-devices",
						Devices: []*podresourcesapi.ContainerDevices{
							{
								ResourceName: "fancydev",
								DeviceIds:    []string{"dev-1", "dev-2"},
							},
						},
					},
				},
			},
		},
		{
			name: "have devices with topology",
			pr: &podresourcesapi.PodResources{
				Name:      "highperf-devs-with-topology",
				Namespace: "exclusive-resources",
				Containers: []*podresourcesapi.ContainerResources{
					{
						Name: "require-devices",
						Devices: []*podresourcesapi.ContainerDevices{
							{
								ResourceName: "fancydev",
								DeviceIds:    []string{"dev-1", "dev-2"},
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := VerifyAlwaysPass(tc.pr)
			if !got.Allow {
				t.Fatalf("alwayspass failed")
			}
		})
	}
}

func TestApply(t *testing.T) {
	podOne := &podresourcesapi.PodResources{
		Name:      "one-container",
		Namespace: "default",
		Containers: []*podresourcesapi.ContainerResources{
			{Name: "c0"},
		},
	}
	podTwo := &podresourcesapi.PodResources{
		Name:      "two-containers",
		Namespace: "default",
		Containers: []*podresourcesapi.ContainerResources{
			{Name: "c0"},
			{Name: "c1"},
		},
	}
	pods := []*podresourcesapi.PodResources{podOne, podTwo}

	t.Run("verifyFunc always returns Allow false", func(t *testing.T) {
		got := Apply(pods, func(*podresourcesapi.PodResources) Result {
			return Result{Allow: false}
		})
		assert.Empty(t, got)
		assert.NotNil(t, got)
	})

	t.Run("verifyFunc always returns Allow true", func(t *testing.T) {
		got := Apply(pods, VerifyAlwaysPass)
		if d := cmp.Diff(pods, got, protocmp.Transform()); d != "" {
			t.Fatalf("unexpected diff (-want +got):\n%s", d)
		}
	})

	t.Run("verifyFunc allows only pods with at least two containers", func(t *testing.T) {
		verifyAtLeastTwoContainers := func(pr *podresourcesapi.PodResources) Result {
			return Result{Allow: len(pr.GetContainers()) >= 2}
		}
		got := Apply(pods, verifyAtLeastTwoContainers)
		want := []*podresourcesapi.PodResources{podTwo}
		if d := cmp.Diff(want, got, protocmp.Transform()); d != "" {
			t.Fatalf("unexpected diff (-want +got):\n%s", d)
		}
	})
}
