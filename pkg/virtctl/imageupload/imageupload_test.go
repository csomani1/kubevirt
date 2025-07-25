/*
 * This file is part of the KubeVirt project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright The KubeVirt Authors.
 *
 */

package imageupload_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	fakek8sclient "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	instancetypeapi "kubevirt.io/api/instancetype"
	fakecdiclient "kubevirt.io/client-go/containerizeddataimporter/fake"
	"kubevirt.io/client-go/kubecli"
	cdiv1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"

	"kubevirt.io/kubevirt/pkg/virtctl/imageupload"
	"kubevirt.io/kubevirt/pkg/virtctl/testing"
	"kubevirt.io/kubevirt/tests/libstorage"
)

const (
	commandName                     = "image-upload"
	uploadRequestAnnotation         = "cdi.kubevirt.io/storage.upload.target"
	forceImmediateBindingAnnotation = "cdi.kubevirt.io/storage.bind.immediate.requested"
	contentTypeAnnotation           = "cdi.kubevirt.io/storage.contentType"
	podPhaseAnnotation              = "cdi.kubevirt.io/storage.pod.phase"
	podReadyAnnotation              = "cdi.kubevirt.io/storage.pod.ready"
	deleteAfterCompletionAnnotation = "cdi.kubevirt.io/storage.deleteAfterCompletion"
	UsePopulatorAnnotation          = "cdi.kubevirt.io/storage.usePopulator"
	PVCPrimeNameAnnotation          = "cdi.kubevirt.io/storage.populator.pvcPrime"
	labelApplyStorageProfile        = "cdi.kubevirt.io/applyStorageProfile"
)

const (
	targetNamespace         = "default"
	targetName              = "test-volume"
	pvcSize                 = "500Mi"
	dvSize                  = "500Mi"
	configName              = "config"
	defaultInstancetypeName = "instancetype"
	defaultInstancetypeKind = "VirtualMachineInstancetype"
	defaultPreferenceName   = "preference"
	defaultPreferenceKind   = "VirtualMachinePreference"
)

var _ = Describe("ImageUpload", func() {

	var (
		ctrl       *gomock.Controller
		kubeClient *fakek8sclient.Clientset
		cdiClient  *fakecdiclient.Clientset
		server     *httptest.Server
		g          *errgroup.Group

		dvCreateCalled  = atomic.Bool{}
		pvcCreateCalled = atomic.Bool{}
		updateCalled    = atomic.Bool{}

		imagePath       string
		archiveFilePath string
	)

	BeforeEach(func(ctx context.Context) {
		g, _ = errgroup.WithContext(ctx)
		ctrl = gomock.NewController(GinkgoT())
		kubecli.GetKubevirtClientFromClientConfig = kubecli.GetMockKubevirtClientFromClientConfig
		kubecli.MockKubevirtClientInstance = kubecli.NewMockKubevirtClient(ctrl)

		imageFile, err := os.CreateTemp("", "test_image")
		Expect(err).ToNot(HaveOccurred())

		_, err = imageFile.Write([]byte("hello world"))
		Expect(err).ToNot(HaveOccurred())
		defer imageFile.Close()

		imagePath = imageFile.Name()

		archiveFile, err := os.CreateTemp("", "archive")
		Expect(err).ToNot(HaveOccurred())
		defer archiveFile.Close()
		archiveFilePath = archiveFile.Name()

		libstorage.ArchiveToFile(archiveFile, imagePath)
	})

	AfterEach(func() {
		os.Remove(imagePath)
		os.Remove(archiveFilePath)
		Expect(g.Wait()).To(Succeed())
	})

	pvcSpec := func() *v1.PersistentVolumeClaim {
		quantity, _ := resource.ParseQuantity(pvcSize)

		pvc := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:        targetName,
				Namespace:   "default",
				Annotations: map[string]string{},
			},
			Spec: v1.PersistentVolumeClaimSpec{
				Resources: v1.VolumeResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceStorage: quantity,
					},
				},
			},
		}

		return pvc
	}

	pvcSpecNoAnnotationMap := func() *v1.PersistentVolumeClaim {
		quantity, _ := resource.ParseQuantity(pvcSize)

		pvc := &v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      targetName,
				Namespace: "default",
			},
			Spec: v1.PersistentVolumeClaimSpec{
				Resources: v1.VolumeResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceStorage: quantity,
					},
				},
			},
		}

		return pvc
	}

	pvcSpecWithUploadAnnotation := func() *v1.PersistentVolumeClaim {
		spec := pvcSpec()
		spec.Annotations = map[string]string{
			uploadRequestAnnotation: "",
			podPhaseAnnotation:      "Running",
			podReadyAnnotation:      "true",
		}
		return spec
	}

	pvcSpecWithUploadSucceeded := func() *v1.PersistentVolumeClaim {
		spec := pvcSpec()
		spec.Annotations = map[string]string{
			uploadRequestAnnotation: "",
			podPhaseAnnotation:      "Succeeded",
			podReadyAnnotation:      "false",
		}
		return spec
	}

	pvcSpecWithGarbageCollection := func() *v1.PersistentVolumeClaim {
		spec := pvcSpec()
		spec.Annotations = map[string]string{
			deleteAfterCompletionAnnotation: "true",
			uploadRequestAnnotation:         "",
			podPhaseAnnotation:              "Succeeded",
			podReadyAnnotation:              "false",
		}
		return spec
	}

	dvSpecWithPhase := func(phase cdiv1.DataVolumePhase) *cdiv1.DataVolume {
		dv := &cdiv1.DataVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:      targetName,
				Namespace: "default",
			},
			TypeMeta: metav1.TypeMeta{
				APIVersion: cdiv1.SchemeGroupVersion.String(),
				Kind:       "DataVolume",
			},
			Spec: cdiv1.DataVolumeSpec{
				Source: &cdiv1.DataVolumeSource{Upload: &cdiv1.DataVolumeSourceUpload{}},
				PVC:    &v1.PersistentVolumeClaimSpec{},
			},
			Status: cdiv1.DataVolumeStatus{Phase: phase},
		}
		return dv
	}

	addPodPhaseAnnotation := func() error {
		defer GinkgoRecover()
		time.Sleep(10 * time.Millisecond)
		pvc, err := kubeClient.CoreV1().PersistentVolumeClaims(targetNamespace).Get(context.Background(), targetName, metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())
		pvc.Annotations[podPhaseAnnotation] = "Running"
		pvc.Annotations[podReadyAnnotation] = "true"
		pvc, err = kubeClient.CoreV1().PersistentVolumeClaims(targetNamespace).Update(context.Background(), pvc, metav1.UpdateOptions{})
		if err != nil {
			fmt.Fprintf(GinkgoWriter, "Error: %v\n", err)
		}
		Expect(err).ToNot(HaveOccurred())
		return nil
	}

	addDvPhase := func() error {
		defer GinkgoRecover()
		time.Sleep(10 * time.Millisecond)
		dv, err := cdiClient.CdiV1beta1().DataVolumes(targetNamespace).Get(context.Background(), targetName, metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())
		dv.Status.Phase = cdiv1.UploadReady
		dv, err = cdiClient.CdiV1beta1().DataVolumes(targetNamespace).Update(context.Background(), dv, metav1.UpdateOptions{})
		if err != nil {
			fmt.Fprintf(GinkgoWriter, "Error: %v\n", err)
		}
		Expect(err).ToNot(HaveOccurred())
		return nil
	}

	createPVC := func(dv *cdiv1.DataVolume) {
		defer GinkgoRecover()
		time.Sleep(10 * time.Millisecond)
		pvc := pvcSpecWithUploadAnnotation()

		if dv.Spec.ContentType == cdiv1.DataVolumeArchive {
			pvc.Annotations[contentTypeAnnotation] = string(cdiv1.DataVolumeArchive)
		}
		pvc.Spec.VolumeMode = getVolumeMode(dv.Spec)
		pvc.Spec.AccessModes = append([]v1.PersistentVolumeAccessMode(nil), getAccessModes(dv.Spec)...)
		pvc.Spec.StorageClassName = getStorageClassName(dv.Spec)
		pvc, err := kubeClient.CoreV1().PersistentVolumeClaims(targetNamespace).Create(context.Background(), pvc, metav1.CreateOptions{})
		Expect(err).ToNot(HaveOccurred())
	}

	addReactors := func() {
		cdiClient.Fake.PrependReactor("create", "datavolumes", func(action k8stesting.Action) (bool, runtime.Object, error) {
			create, ok := action.(k8stesting.CreateAction)
			Expect(ok).To(BeTrue())

			dv, ok := create.GetObject().(*cdiv1.DataVolume)
			Expect(ok).To(BeTrue())
			Expect(dv.Name).To(Equal(targetName))

			Expect(dvCreateCalled.Load()).To(BeFalse())
			dvCreateCalled.Store(true)

			g.Go(func() error {
				createPVC(dv)
				return nil
			})
			g.Go(addDvPhase)

			return false, nil, nil
		})

		kubeClient.Fake.PrependReactor("create", "persistentvolumeclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
			create, ok := action.(k8stesting.CreateAction)
			Expect(ok).To(BeTrue())

			pvc, ok := create.GetObject().(*v1.PersistentVolumeClaim)
			Expect(ok).To(BeTrue())
			Expect(pvc.Name).To(Equal(targetName))

			Expect(pvcCreateCalled.Load()).To(BeFalse())
			pvcCreateCalled.Store(true)

			if !dvCreateCalled.Load() {
				g.Go(addPodPhaseAnnotation)
			}

			return false, nil, nil
		})

		kubeClient.Fake.PrependReactor("update", "persistentvolumeclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
			update, ok := action.(k8stesting.UpdateAction)
			Expect(ok).To(BeTrue())

			pvc, ok := update.GetObject().(*v1.PersistentVolumeClaim)
			Expect(ok).To(BeTrue())
			Expect(pvc.Name).To(Equal(targetName))

			if !dvCreateCalled.Load() && !pvcCreateCalled.Load() && !updateCalled.Load() {
				g.Go(addPodPhaseAnnotation)
			}

			updateCalled.Store(true)

			return false, nil, nil
		})

		kubeClient.Fake.PrependReactor("get", "storageclasses", func(action k8stesting.Action) (bool, runtime.Object, error) {
			_, ok := action.(k8stesting.GetAction)
			Expect(ok).To(BeTrue())
			return true, nil, nil
		})
	}

	validateDvStorageSpec := func(spec cdiv1.DataVolumeSpec, mode v1.PersistentVolumeMode) {
		resource, ok := getResourceRequestedStorageSize(spec)

		Expect(ok).To(BeTrue())
		Expect(resource.String()).To(Equal(pvcSize))

		volumeMode := getVolumeMode(spec)
		if volumeMode == nil {
			vm := v1.PersistentVolumeFilesystem
			volumeMode = &vm
		}
		Expect(mode).To(Equal(*volumeMode))
	}

	validatePVCSpec := func(spec *v1.PersistentVolumeClaimSpec, mode v1.PersistentVolumeMode) {
		resource, ok := spec.Resources.Requests[v1.ResourceStorage]

		Expect(ok).To(BeTrue())
		Expect(resource.String()).To(Equal(pvcSize))

		volumeMode := spec.VolumeMode
		if volumeMode == nil {
			vm := v1.PersistentVolumeFilesystem
			volumeMode = &vm
		}
		Expect(mode).To(Equal(*volumeMode))
	}

	validatePVCArgs := func(mode v1.PersistentVolumeMode) {
		pvc, err := kubeClient.CoreV1().PersistentVolumeClaims(targetNamespace).Get(context.Background(), targetName, metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())

		_, ok := pvc.Annotations[uploadRequestAnnotation]
		Expect(ok).To(BeTrue())

		validatePVCSpec(&pvc.Spec, mode)
	}

	validateArchivePVC := func() {
		pvc, err := kubeClient.CoreV1().PersistentVolumeClaims(targetNamespace).Get(context.Background(), targetName, metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())

		_, ok := pvc.Annotations[uploadRequestAnnotation]
		Expect(ok).To(BeTrue())
		contentType, ok := pvc.Annotations[contentTypeAnnotation]
		Expect(ok).To(BeTrue())
		Expect(contentType).To(Equal(string(cdiv1.DataVolumeArchive)))

		validatePVCSpec(&pvc.Spec, v1.PersistentVolumeFilesystem)
	}

	validatePVC := func() {
		validatePVCArgs(v1.PersistentVolumeFilesystem)
	}

	validateBlockPVC := func() {
		validatePVCArgs(v1.PersistentVolumeBlock)
	}

	validateDataVolumeArgs := func(dv *cdiv1.DataVolume, mode v1.PersistentVolumeMode) {
		validateDvStorageSpec(dv.Spec, mode)
	}

	validateArchiveDataVolume := func() {
		dv, err := cdiClient.CdiV1beta1().DataVolumes(targetNamespace).Get(context.Background(), targetName, metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())

		validateDvStorageSpec(dv.Spec, v1.PersistentVolumeFilesystem)
		Expect(dv.Spec.ContentType).To(Equal(cdiv1.DataVolumeArchive))
	}

	validateDataVolume := func() {
		dv, err := cdiClient.CdiV1beta1().DataVolumes(targetNamespace).Get(context.Background(), targetName, metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())

		validateDataVolumeArgs(dv, v1.PersistentVolumeFilesystem)
	}

	validateBlockDataVolume := func() {
		dv, err := cdiClient.CdiV1beta1().DataVolumes(targetNamespace).Get(context.Background(), targetName, metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())

		validateDataVolumeArgs(dv, v1.PersistentVolumeBlock)
	}

	validateDataVolumeWithForceBind := func() {
		dv, err := cdiClient.CdiV1beta1().DataVolumes(targetNamespace).Get(context.Background(), targetName, metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())
		_, ok := dv.Annotations[forceImmediateBindingAnnotation]
		Expect(ok).To(BeTrue(), "storage.bind.immediate.requested annotation")

		validateDataVolumeArgs(dv, v1.PersistentVolumeFilesystem)
	}

	validateDefaultInstancetypeLabels := func(labels map[string]string) {
		Expect(labels).ToNot(BeNil())
		Expect(labels).To(HaveKeyWithValue(instancetypeapi.DefaultInstancetypeLabel, defaultInstancetypeName))
		Expect(labels).To(HaveKeyWithValue(instancetypeapi.DefaultInstancetypeKindLabel, defaultInstancetypeKind))
		Expect(labels).To(HaveKeyWithValue(instancetypeapi.DefaultPreferenceLabel, defaultPreferenceName))
		Expect(labels).To(HaveKeyWithValue(instancetypeapi.DefaultPreferenceKindLabel, defaultPreferenceKind))
	}

	validatePVCDefaultInstancetypeLabels := func() {
		pvc, err := kubeClient.CoreV1().PersistentVolumeClaims(targetNamespace).Get(context.Background(), targetName, metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())
		validateDefaultInstancetypeLabels(pvc.ObjectMeta.Labels)
		// This label is applied by default to all imageupload-created PVCs
		Expect(pvc.ObjectMeta.Labels).To(HaveKeyWithValue(labelApplyStorageProfile, "true"))
	}

	validateDataVolumeDefaultInstancetypeLabels := func() {
		dv, err := cdiClient.CdiV1beta1().DataVolumes(targetNamespace).Get(context.Background(), targetName, metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())
		validateDefaultInstancetypeLabels(dv.ObjectMeta.Labels)
	}

	expectedStorageClassMatchesActual := func(storageClass string) {
		pvc, err := kubeClient.CoreV1().PersistentVolumeClaims(targetNamespace).Get(context.Background(), targetName, metav1.GetOptions{})
		Expect(err).ToNot(HaveOccurred())
		_, ok := pvc.Annotations[uploadRequestAnnotation]
		Expect(ok).To(BeTrue())
		Expect(storageClass).To(Equal(*pvc.Spec.StorageClassName))
	}

	createCDIConfig := func() *cdiv1.CDIConfig {
		return &cdiv1.CDIConfig{
			ObjectMeta: metav1.ObjectMeta{
				Name: configName,
			},
			Spec: cdiv1.CDIConfigSpec{
				UploadProxyURLOverride: nil,
			},
			Status: cdiv1.CDIConfigStatus{
				UploadProxyURL: nil,
			},
		}
	}

	updateCDIConfig := func(config *cdiv1.CDIConfig) {
		_, err := cdiClient.CdiV1beta1().CDIConfigs().Update(context.Background(), config, metav1.UpdateOptions{})
		if err != nil {
			fmt.Fprintf(GinkgoWriter, "Error: %v\n", err)
		}
		Expect(err).ToNot(HaveOccurred())
	}

	waitProcessingComplete := func(client kubernetes.Interface, cmd *cobra.Command, namespace, name string, interval, timeout time.Duration) error {
		return nil
	}

	testInitAsyncWithCdiObjects := func(statusCode int, async bool, kubeobjects []runtime.Object, cdiobjects []runtime.Object) {
		dvCreateCalled.Store(false)
		pvcCreateCalled.Store(false)
		updateCalled.Store(false)

		config := createCDIConfig()
		cdiobjects = append(cdiobjects, config)

		kubeClient = fakek8sclient.NewSimpleClientset(kubeobjects...)
		cdiClient = fakecdiclient.NewSimpleClientset(cdiobjects...)

		kubecli.MockKubevirtClientInstance.EXPECT().CoreV1().Return(kubeClient.CoreV1()).AnyTimes()
		kubecli.MockKubevirtClientInstance.EXPECT().CdiClient().Return(cdiClient).AnyTimes()
		kubecli.MockKubevirtClientInstance.EXPECT().StorageV1().Return(kubeClient.StorageV1()).AnyTimes()

		addReactors()

		server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "HEAD" {
				if async {
					w.WriteHeader(http.StatusOK)
				} else {
					w.WriteHeader(http.StatusNotFound)
				}
				return
			}
			w.WriteHeader(statusCode)
		}))
		config.Status.UploadProxyURL = &server.URL
		updateCDIConfig(config)

		imageupload.UploadProcessingCompleteFunc = waitProcessingComplete
		imageupload.GetHTTPClientFn = func(bool) *http.Client {
			return server.Client()
		}
	}

	testInitAsync := func(statusCode int, async bool, kubeobjects ...runtime.Object) {
		testInitAsyncWithCdiObjects(statusCode, async, kubeobjects, nil)
	}

	testInit := func(statusCode int, kubeobjects ...runtime.Object) {
		testInitAsync(statusCode, true, kubeobjects...)
	}

	testDone := func() {
		imageupload.GetHTTPClientFn = imageupload.GetHTTPClient
		server.Close()
	}

	Context("Successful upload to PVC", func() {

		It("PVC does not exist deprecated args", func() {
			testInit(http.StatusOK)
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "--pvc-name", targetName, "--pvc-size", pvcSize,
				"--uploadproxy-url", server.URL, "--insecure", "--image-path", imagePath)
			Expect(cmd()).To(Succeed())
			Expect(pvcCreateCalled.Load()).To(BeTrue())
			validatePVC()
		})

		It("PVC exists deprecated args", func() {
			testInit(http.StatusOK, pvcSpec())
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "--pvc-name", targetName, "--no-create",
				"--uploadproxy-url", server.URL, "--insecure", "--image-path", imagePath)
			Expect(cmd()).To(Succeed())
			Expect(pvcCreateCalled.Load()).To(BeFalse())
			validatePVC()
		})

		DescribeTable("DV does not exist", func(async bool) {
			testInitAsync(http.StatusOK, async)
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "dv", targetName, "--size", pvcSize,
				"--uploadproxy-url", server.URL, "--insecure", "--image-path", imagePath)
			Expect(cmd()).To(Succeed())
			Expect(dvCreateCalled.Load()).To(BeTrue())
			validatePVC()
			validateDataVolume()
		},
			Entry("DV does not exist, async", true),
			Entry("DV does not exist sync", false),
		)

		It("upload archive file DV doest not exist", func() {
			testInit(http.StatusOK)
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "dv", targetName, "--size", pvcSize,
				"--uploadproxy-url", server.URL, "--insecure", "--archive-path", archiveFilePath)
			Expect(cmd()).To(Succeed())
			Expect(dvCreateCalled.Load()).To(BeTrue())
			validateArchivePVC()
			validateArchiveDataVolume()
		})

		It("DV does not exist --pvc-size", func() {
			testInit(http.StatusOK)
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "dv", targetName, "--pvc-size", pvcSize,
				"--uploadproxy-url", server.URL, "--insecure", "--image-path", imagePath)
			Expect(cmd()).To(Succeed())
			Expect(dvCreateCalled.Load()).To(BeTrue())
			validatePVC()
			validateDataVolume()
		})

		It("DV does not exist --force-bind", func() {
			testInit(http.StatusOK)
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "dv", targetName, "--pvc-size", pvcSize,
				"--uploadproxy-url", server.URL, "--insecure", "--image-path", imagePath, "--force-bind")
			Expect(cmd()).To(Succeed())
			Expect(dvCreateCalled.Load()).To(BeTrue())
			validatePVC()
			validateDataVolumeWithForceBind()
		})

		It("DV does not exist and --no-create", func() {
			testInit(http.StatusOK)
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "dv", targetName, "--pvc-size", pvcSize,
				"--uploadproxy-url", server.URL, "--insecure", "--image-path", imagePath, "--no-create")
			err := cmd()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal(fmt.Sprintf("persistentvolumeclaims %q not found", targetName)))
			Expect(dvCreateCalled.Load()).To(BeFalse())
		})

		It("Use CDI Config UploadProxyURL", func() {
			testInit(http.StatusOK)
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "dv", targetName, "--size", pvcSize,
				"--insecure", "--image-path", imagePath)
			Expect(cmd()).To(Succeed())
			Expect(dvCreateCalled.Load()).To(BeTrue())
			validatePVC()
			validateDataVolume()
		})

		DescribeTable("Create a VolumeMode=Block PVC", func(flag string) {
			testInit(http.StatusOK)
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "dv", targetName, "--size", pvcSize,
				"--insecure", "--image-path", imagePath, "--block-volume")
			Expect(cmd()).To(Succeed())
			Expect(dvCreateCalled.Load()).To(BeTrue())
			validateBlockPVC()
			validateBlockDataVolume()
		},
			Entry("using deprecated flag", "--block-volume"),
			Entry("using VolumeMode flag", "-volume-mode=block"),
		)

		It("Create a VolumeMode=Filesystem PVC using volume-mode flag", func() {
			testInit(http.StatusOK)
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "dv", targetName, "--size", pvcSize,
				"--insecure", "--image-path", imagePath, "--volume-mode", "filesystem")
			Expect(cmd()).To(Succeed())
			Expect(dvCreateCalled.Load()).To(BeTrue())
			validatePVC()
			validateDataVolume()
		})

		It("Create a non-default storage class PVC", func() {
			testInit(http.StatusOK)
			expectedStorageClass := "non-default-sc"
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "dv", targetName, "--size", pvcSize,
				"--insecure", "--image-path", imagePath, "--storage-class", expectedStorageClass)
			Expect(cmd()).To(Succeed())
			Expect(dvCreateCalled.Load()).To(BeTrue())
			expectedStorageClassMatchesActual(expectedStorageClass)
		})

		DescribeTable("PVC does not exist", func(async bool) {
			testInitAsync(http.StatusOK, async)
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "pvc", targetName, "--size", pvcSize,
				"--uploadproxy-url", server.URL, "--insecure", "--image-path", imagePath)
			Expect(cmd()).To(Succeed())
			Expect(pvcCreateCalled.Load()).To(BeTrue())
			validatePVC()
		},
			Entry("PVC does not exist, async", true),
			Entry("PVC does not exist sync", false),
		)

		DescribeTable("PVC does exist", func(pvc *v1.PersistentVolumeClaim) {
			testInit(http.StatusOK, pvc)
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "pvc", targetName,
				"--uploadproxy-url", server.URL, "--no-create", "--insecure", "--image-path", imagePath)
			Expect(cmd()).To(Succeed())
			Expect(pvcCreateCalled.Load()).To(BeFalse())
			validatePVC()
		},
			Entry("PVC with upload annotation", pvcSpecWithUploadAnnotation()),
			Entry("PVC without upload annotation", pvcSpec()),
			Entry("PVC without upload annotation and no annotation map", pvcSpecNoAnnotationMap()),
		)

		It("Archive upload PVC does not exist", func() {
			testInit(http.StatusOK)
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "pvc", targetName, "--size", pvcSize,
				"--uploadproxy-url", server.URL, "--insecure", "--archive-path", archiveFilePath)
			Expect(cmd()).To(Succeed())
			Expect(pvcCreateCalled.Load()).To(BeTrue())
			validateArchivePVC()
		})

		It("Archive upload PVC exist", func() {
			pvc := pvcSpecWithUploadAnnotation()
			pvc.Annotations[contentTypeAnnotation] = string(cdiv1.DataVolumeArchive)
			testInit(http.StatusOK, pvc)
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "pvc", targetName,
				"--uploadproxy-url", server.URL, "--no-create", "--insecure", "--archive-path", archiveFilePath)
			Expect(cmd()).To(Succeed())
			Expect(pvcCreateCalled.Load()).To(BeFalse())
			validateArchivePVC()
		})

		It("Show error when uploading to ReadOnly volume", func() {
			testInit(http.StatusOK)
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "dv", targetName, "--size", pvcSize,
				"--insecure", "--image-path", imagePath, "--access-mode", string(v1.ReadOnlyMany))
			err := cmd()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(Equal("cannot upload to a readonly volume, use either ReadWriteOnce or ReadWriteMany if supported"))
			Expect(dvCreateCalled.Load()).To(BeFalse())
		})

		It("Should set default instance type and preference labels on DataVolume", func() {
			testInit(http.StatusOK)
			cmd := testing.NewRepeatableVirtctlCommand(
				commandName, "dv", targetName,
				"--size", pvcSize,
				"--uploadproxy-url", server.URL,
				"--insecure",
				"--image-path", imagePath,
				"--default-instancetype", defaultInstancetypeName,
				"--default-instancetype-kind", defaultInstancetypeKind,
				"--default-preference", defaultPreferenceName,
				"--default-preference-kind", defaultPreferenceKind,
			)
			Expect(cmd()).To(Succeed())
			Expect(dvCreateCalled.Load()).To(BeTrue())
			validatePVC()
			validateDataVolume()
			validateDataVolumeDefaultInstancetypeLabels()
		})

		It("Should set default instance type and preference labels on PVC", func() {
			testInit(http.StatusOK)
			cmd := testing.NewRepeatableVirtctlCommand(
				commandName, "pvc", targetName,
				"--size", pvcSize,
				"--uploadproxy-url", server.URL,
				"--insecure",
				"--image-path", imagePath,
				"--default-instancetype", defaultInstancetypeName,
				"--default-instancetype-kind", defaultInstancetypeKind,
				"--default-preference", defaultPreferenceName,
				"--default-preference-kind", defaultPreferenceKind,
			)
			Expect(cmd()).To(Succeed())
			Expect(pvcCreateCalled.Load()).To(BeTrue())
			validatePVC()
			validatePVCDefaultInstancetypeLabels()
		})

		It("Should create DataSource pointing to the PVC", func() {
			testInit(http.StatusOK)
			cmd := testing.NewRepeatableVirtctlCommand(
				commandName, "dv", targetName,
				"--size", dvSize,
				"--uploadproxy-url", server.URL,
				"--insecure",
				"--force-bind",
				"--datasource",
				"--image-path", imagePath,
				"--default-instancetype", "fake.large",
				"--default-instancetype-kind", "fake.large",
				"--default-preference", "fake.centos",
				"--default-preference-kind", "fake.centos",
			)
			Expect(cmd()).To(Succeed())
			Expect(pvcCreateCalled.Load()).To(BeTrue())

			ds, err := cdiClient.CdiV1beta1().DataSources(targetNamespace).Get(context.Background(), targetName, metav1.GetOptions{})
			Expect(err).ToNot(HaveOccurred())

			assertDataSource(ds, targetName, targetNamespace)
		})

		It("Should patch DataSource pointing to the PVC", func() {
			testInit(http.StatusOK)

			ds := &cdiv1.DataSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      targetName,
					Namespace: targetNamespace,
					Labels:    map[string]string{},
				},
				Spec: cdiv1.DataSourceSpec{
					Source: cdiv1.DataSourceSource{
						PVC: &cdiv1.DataVolumeSourcePVC{
							Name:      "",
							Namespace: "",
						},
					},
				},
			}
			_, err := cdiClient.CdiV1beta1().DataSources(targetNamespace).Create(context.Background(), ds, metav1.CreateOptions{})
			Expect(err).ToNot(HaveOccurred())

			cmd := testing.NewRepeatableVirtctlCommand(
				commandName, "dv", targetName,
				"--size", dvSize,
				"--uploadproxy-url", server.URL,
				"--insecure",
				"--force-bind",
				"--datasource",
				"--image-path", imagePath,
				"--default-instancetype", "fake.large",
				"--default-instancetype-kind", "fake.large",
				"--default-preference", "fake.centos",
				"--default-preference-kind", "fake.centos",
			)
			Expect(cmd()).To(Succeed())
			Expect(pvcCreateCalled.Load()).To(BeTrue())

			ds, err = cdiClient.CdiV1beta1().DataSources(targetNamespace).Get(context.Background(), targetName, metav1.GetOptions{})
			Expect(err).ToNot(HaveOccurred())

			assertDataSource(ds, targetName, targetNamespace)
		})

		DescribeTable("Should retry on server returning error code", func(expected int, extraArgs ...string) {
			testInit(http.StatusOK)

			attempts := 1
			server = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if attempts < expected {
					w.WriteHeader(http.StatusBadGateway)
					attempts++
				} else {
					w.WriteHeader(http.StatusOK)
				}
			}))

			args := append([]string{
				commandName,
				"--pvc-name", targetName,
				"--pvc-size", pvcSize,
				"--uploadproxy-url", server.URL,
				"--insecure",
				"--image-path", imagePath,
			}, extraArgs...)
			cmd := testing.NewRepeatableVirtctlCommand(args...)
			Expect(cmd()).To(Succeed())
			Expect(pvcCreateCalled.Load()).To(BeTrue())
			validatePVC()

			Expect(attempts).To(Equal(expected))
		},
			Entry("with default configuration", 6),
			Entry("with explicit amount of retries", 11, "--retry=10"),
		)

		AfterEach(func() {
			testDone()
		})
	})

	Context("Upload fails", func() {
		It("DV already uploaded and garbagecollected", func() {
			testInit(http.StatusOK, pvcSpecWithGarbageCollection())
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "dv", targetName, "--size", pvcSize,
				"--uploadproxy-url", server.URL, "--insecure", "--image-path", imagePath)
			err := cmd()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).Should(ContainSubstring("DataVolume already garbage-collected"))
		})

		It("PVC already exists independently of the DV", func() {
			testInit(http.StatusOK, pvcSpecWithUploadSucceeded())
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "dv", targetName, "--size", pvcSize,
				"--uploadproxy-url", server.URL, "--insecure", "--image-path", imagePath)
			err := cmd()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).Should(ContainSubstring("No DataVolume is associated with the existing PVC"))
		})

		It("PVC exists without archive contentType for archive upload", func() {
			testInit(http.StatusOK, pvcSpecWithUploadAnnotation())
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "pvc", targetName,
				"--uploadproxy-url", server.URL, "--no-create", "--insecure", "--archive-path", archiveFilePath)
			err := cmd()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).Should(ContainSubstring("doesn't have archive contentType annotation"))
		})

		It("DV is using populators and target PVC is bound", func() {
			dv := dvSpecWithPhase(cdiv1.UploadReady)
			dv.Annotations = map[string]string{UsePopulatorAnnotation: "true"}
			pvc := pvcSpecWithUploadAnnotation()
			pvc.Status.Phase = v1.ClaimBound
			testInitAsyncWithCdiObjects(
				http.StatusOK,
				true,
				[]runtime.Object{pvc},
				[]runtime.Object{dv},
			)
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "dv", targetName, "--size", pvcSize,
				"--uploadproxy-url", server.URL, "--insecure", "--archive-path", archiveFilePath)
			err := cmd()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).Should(ContainSubstring("already successfully populated"))
		})

		It("DV is using populators but PVC has no PVC Prime annotation", func() {
			dv := dvSpecWithPhase(cdiv1.UploadReady)
			dv.Annotations = map[string]string{UsePopulatorAnnotation: "true"}
			testInitAsyncWithCdiObjects(
				http.StatusOK,
				true,
				[]runtime.Object{pvcSpecWithUploadAnnotation()},
				[]runtime.Object{dv},
			)
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "dv", targetName, "--size", pvcSize,
				"--uploadproxy-url", server.URL, "--insecure", "--archive-path", archiveFilePath)
			err := cmd()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).Should(ContainSubstring("Unable to get PVC Prime name from PVC"))
		})

		It("DV is using populators but PVC Prime doesn't exist", func() {
			dv := dvSpecWithPhase(cdiv1.UploadReady)
			dv.Annotations = map[string]string{UsePopulatorAnnotation: "true"}
			pvc := pvcSpecWithUploadAnnotation()
			pvc.Annotations = map[string]string{PVCPrimeNameAnnotation: "pvc-prime-name"}
			testInitAsyncWithCdiObjects(
				http.StatusOK,
				true,
				[]runtime.Object{pvc},
				[]runtime.Object{dv},
			)
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "dv", targetName, "--size", pvcSize,
				"--uploadproxy-url", server.URL, "--insecure", "--archive-path", archiveFilePath)
			err := cmd()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).Should(ContainSubstring("Unable to get PVC Prime"))
		})

		DescribeTable("when DV in phase", func(phase cdiv1.DataVolumePhase, forcebind bool) {
			testInitAsyncWithCdiObjects(
				http.StatusOK,
				true,
				[]runtime.Object{pvcSpecWithUploadAnnotation()},
				[]runtime.Object{dvSpecWithPhase(phase)},
			)
			if forcebind {
				cmd := testing.NewRepeatableVirtctlCommand(commandName, "dv", targetName, "--size", pvcSize,
					"--uploadproxy-url", server.URL, "--insecure", "--image-path", imagePath, "--force-bind")
				g.Go(addDvPhase)
				Expect(cmd()).To(Succeed())
				Expect(pvcCreateCalled.Load()).To(BeFalse())
				Expect(dvCreateCalled.Load()).To(BeFalse())
			} else {
				cmd := testing.NewRepeatableVirtctlCommand(commandName, "dv", targetName, "--size", pvcSize,
					"--uploadproxy-url", server.URL, "--insecure", "--image-path", imagePath)
				err := cmd()
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).Should(ContainSubstring(fmt.Sprintf("cannot upload to DataVolume in %s phase", phase)))
			}
		},
			Entry("WaitForFirstConsumer should fail without force-bind flag", cdiv1.WaitForFirstConsumer, false),
			Entry("WaitForFirstConsumer should succeed with force-bind flag", cdiv1.WaitForFirstConsumer, true),
			Entry("PendingPopulation should fail without force-bind flag", cdiv1.PendingPopulation, false),
			Entry("PendingPopulation should succeed with force-bind flag", cdiv1.WaitForFirstConsumer, true),
		)

		It("uploadProxyURL not configured", func() {
			testInit(http.StatusOK)
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "dv", targetName, "--size", pvcSize,
				"--insecure", "--image-path", imagePath)
			config, err := cdiClient.CdiV1beta1().CDIConfigs().Get(context.Background(), configName, metav1.GetOptions{})
			Expect(err).ToNot(HaveOccurred())
			config.Status.UploadProxyURL = nil
			updateCDIConfig(config)
			Expect(cmd()).NotTo(Succeed())
		})

		It("Upload fails", func() {
			testInit(http.StatusInternalServerError)
			cmd := testing.NewRepeatableVirtctlCommand(commandName, "dv", targetName, "--size", pvcSize,
				"--uploadproxy-url", server.URL, "--insecure", "--image-path", imagePath)
			Expect(cmd()).NotTo(Succeed())
		})

		DescribeTable("Upload fails when using a nonexistent storageClass", func(resource string) {
			const (
				errFmt              = "storageclasses.storage.k8s.io \"%s\" not found"
				invalidStorageClass = "no-sc"
			)

			testInit(http.StatusInternalServerError)
			kubeClient.Fake.PrependReactor("get", "storageclasses", func(action k8stesting.Action) (bool, runtime.Object, error) {
				_, ok := action.(k8stesting.GetAction)
				Expect(ok).To(BeTrue())
				return true, nil, fmt.Errorf(errFmt, invalidStorageClass)
			})

			err := testing.NewRepeatableVirtctlCommand(commandName,
				resource, targetName,
				"--size", pvcSize,
				"--uploadproxy-url", server.URL,
				"--insecure",
				"--image-path", imagePath,
				"--storage-class", invalidStorageClass,
			)()
			Expect(err).To(MatchError(ContainSubstring(errFmt, invalidStorageClass)))
		},
			Entry("DataVolume", "dv"),
			Entry("PVC", "pvc"),
		)

		DescribeTable("Bad args", func(errString string, args []string) {
			testInit(http.StatusOK)
			args = append([]string{commandName}, args...)
			cmd := testing.NewRepeatableVirtctlCommand(args...)
			err := cmd()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).Should(Equal(errString))
		},
			Entry("No args", "either image-path or archive-path must be provided", []string{}),
			Entry("Missing arg", "expecting two args",
				[]string{"targetName", "--size", pvcSize, "--uploadproxy-url", "https://doesnotexist", "--insecure", "--image-path", "/dev/null"}),
			Entry("No name", "expecting two args",
				[]string{"--size", pvcSize, "--uploadproxy-url", "https://doesnotexist", "--insecure", "--image-path", "/dev/null"}),
			Entry("No size", "when creating a resource, the size must be specified",
				[]string{"dv", targetName, "--uploadproxy-url", "https://doesnotexist", "--insecure", "--image-path", "/dev/null"}),
			Entry("Size invalid", "validation failed for size=500Zb: quantities must match the regular expression '^([+-]?[0-9.]+)([eEinumkKMGTP]*[-+]?[0-9]*)$'",
				[]string{"dv", targetName, "--size", "500Zb", "--uploadproxy-url", "https://doesnotexist", "--insecure", "--image-path", "/dev/null"}),
			Entry("No image path nor archive-path", "either image-path or archive-path must be provided",
				[]string{"dv", targetName, "--size", pvcSize, "--uploadproxy-url", "https://doesnotexist", "--insecure"}),
			Entry("Image path and archive path provided", "cannot handle both image-path and archive-path, provide only one",
				[]string{"dv", targetName, "--size", pvcSize, "--uploadproxy-url", "https://doesnotexist", "--insecure", "--image-path", "/dev/null", "--archive-path", "/dev/null.tar"}),
			Entry("Archive path and block volume true provided", "In archive upload the volume mode should always be filesystem",
				[]string{"dv", targetName, "--size", pvcSize, "--uploadproxy-url", "https://doesnotexist", "--insecure", "--archive-path", "/dev/null.tar", "--block-volume"}),
			Entry("BlockVolume true provided with different volume-mode", "incompatible --volume-mode 'filesystem' and --block-volume",
				[]string{"dv", targetName, "--size", pvcSize, "--uploadproxy-url", "https://doesnotexist", "--insecure", "--archive-path", "/dev/null.tar", "--block-volume", "--volume-mode", "filesystem"}),
			Entry("Invalid volume-mode specified", "Invalid volume mode 'foo'. Valid values are 'block' and 'filesystem'.",
				[]string{"dv", targetName, "--size", pvcSize, "--uploadproxy-url", "https://doesnotexist", "--insecure", "--archive-path", "/dev/null.tar", "--volume-mode", "foo"}),
			Entry("PVC name and args", "cannot use --pvc-name and args",
				[]string{"foo", "--pvc-name", targetName, "--size", pvcSize, "--uploadproxy-url", "https://doesnotexist", "--insecure", "--image-path", "/dev/null"}),
			Entry("Unexpected resource type", "invalid resource type foo",
				[]string{"foo", targetName, "--size", pvcSize, "--uploadproxy-url", "https://doesnotexist", "--insecure", "--image-path", "/dev/null"}),
			Entry("Size twice", "--pvc-size and --size can not be specified at the same time",
				[]string{"dv", targetName, "--size", "500G", "--pvc-size", "50G", "--uploadproxy-url", "https://doesnotexist", "--insecure", "--image-path", "/dev/null"}),
			Entry("--default-instancetype-kind without --default-instancetype", "--default-instancetype must be provided with --default-instancetype-kind",
				[]string{"pvc", targetName, "--size", pvcSize, "--uploadproxy-url", "https://doesnotexist", "--insecure", "--image-path", "/dev/null", "--default-instancetype-kind", "foo"}),
			Entry("--default-preference-kind without --default-preference", "--default-preference must be provided with --default-preference-kind",
				[]string{"pvc", targetName, "--size", pvcSize, "--uploadproxy-url", "https://doesnotexist", "--insecure", "--image-path", "/dev/null", "--default-preference-kind", "foo"}),
			Entry("--default-instancetype with --no-create", "--default-instancetype and --default-preference cannot be used with --no-create",
				[]string{"pvc", targetName, "--size", pvcSize, "--uploadproxy-url", "https://doesnotexist", "--insecure", "--image-path", "/dev/null", "--default-instancetype", "foo", "--no-create"}),
			Entry("--default-preference with --no-create", "--default-instancetype and --default-preference cannot be used with --no-create",
				[]string{"pvc", targetName, "--size", pvcSize, "--uploadproxy-url", "https://doesnotexist", "--insecure", "--image-path", "/dev/null", "--default-preference", "foo", "--no-create"}),
		)

		AfterEach(func() {
			testDone()
		})
	})

	Context("URL validation", func() {
		serverURL := "http://localhost:12345"
		DescribeTable("Server URL validations", func(serverUrl string, expected string) {
			path, err := imageupload.ConstructUploadProxyPath(serverUrl)
			Expect(err).ToNot(HaveOccurred())
			Expect(strings.Compare(path, expected)).To(BeZero())
		},
			Entry("Server URL with trailing slash should pass", serverURL+"/", serverURL+imageupload.UploadProxyURI),
			Entry("Server URL with URI should pass", serverURL+imageupload.UploadProxyURI, serverURL+imageupload.UploadProxyURI),
			Entry("Server URL only should pass", serverURL, serverURL+imageupload.UploadProxyURI),
		)
	})
})

func getResourceRequestedStorageSize(dvSpec cdiv1.DataVolumeSpec) (resource.Quantity, bool) {
	if dvSpec.PVC != nil {
		resource, ok := dvSpec.PVC.Resources.Requests[v1.ResourceStorage]
		return resource, ok
	}
	resource, ok := dvSpec.Storage.Resources.Requests[v1.ResourceStorage]
	return resource, ok
}

func getStorageClassName(dvSpec cdiv1.DataVolumeSpec) *string {
	if dvSpec.PVC != nil {
		return dvSpec.PVC.StorageClassName
	}
	return dvSpec.Storage.StorageClassName
}

func getAccessModes(dvSpec cdiv1.DataVolumeSpec) []v1.PersistentVolumeAccessMode {
	if dvSpec.PVC != nil {
		return dvSpec.PVC.AccessModes
	}
	return dvSpec.Storage.AccessModes
}

func getVolumeMode(dvSpec cdiv1.DataVolumeSpec) *v1.PersistentVolumeMode {
	if dvSpec.PVC != nil {
		return dvSpec.PVC.VolumeMode
	}
	return dvSpec.Storage.VolumeMode
}

func assertDataSource(ds *cdiv1.DataSource, targetName, targetNamespace string) {
	Expect(ds.Labels).To(HaveKeyWithValue(instancetypeapi.DefaultInstancetypeLabel, "fake.large"))
	Expect(ds.Labels).To(HaveKeyWithValue(instancetypeapi.DefaultInstancetypeKindLabel, "fake.large"))
	Expect(ds.Labels).To(HaveKeyWithValue(instancetypeapi.DefaultPreferenceLabel, "fake.centos"))
	Expect(ds.Labels).To(HaveKeyWithValue(instancetypeapi.DefaultPreferenceKindLabel, "fake.centos"))
	Expect(ds.Spec.Source.PVC).ToNot(BeNil())
	Expect(ds.Spec.Source.PVC.Name).To(Equal(targetName))
	Expect(ds.Spec.Source.PVC.Namespace).To(Equal(targetNamespace))
}
