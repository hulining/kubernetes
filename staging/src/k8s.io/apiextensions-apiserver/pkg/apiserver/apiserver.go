/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package apiserver

import (
	"fmt"
	"net/http"
	"time"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/install"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	_ "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/internalclientset"
	_ "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions"
	internalinformers "k8s.io/apiextensions-apiserver/pkg/client/informers/internalversion"
	"k8s.io/apiextensions-apiserver/pkg/controller/apiapproval"
	"k8s.io/apiextensions-apiserver/pkg/controller/establish"
	"k8s.io/apiextensions-apiserver/pkg/controller/finalizer"
	"k8s.io/apiextensions-apiserver/pkg/controller/nonstructuralschema"
	openapicontroller "k8s.io/apiextensions-apiserver/pkg/controller/openapi"
	"k8s.io/apiextensions-apiserver/pkg/controller/status"
	apiextensionsfeatures "k8s.io/apiextensions-apiserver/pkg/features"
	"k8s.io/apiextensions-apiserver/pkg/registry/customresourcedefinition"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/apiserver/pkg/endpoints/discovery"
	genericregistry "k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/pkg/util/webhook"
)

var (
	Scheme = runtime.NewScheme()
	Codecs = serializer.NewCodecFactory(Scheme)

	// if you modify this, make sure you update the crEncoder
	unversionedVersion = schema.GroupVersion{Group: "", Version: "v1"}
	unversionedTypes   = []runtime.Object{
		&metav1.Status{},
		&metav1.WatchEvent{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	}
)

func init() {
	install.Install(Scheme)

	// we need to add the options to empty v1
	metav1.AddToGroupVersion(Scheme, schema.GroupVersion{Group: "", Version: "v1"})

	Scheme.AddUnversionedTypes(unversionedVersion, unversionedTypes...)
}

type ExtraConfig struct {
	CRDRESTOptionsGetter genericregistry.RESTOptionsGetter

	// MasterCount is used to detect whether cluster is HA, and if it is
	// the CRD Establishing will be hold by 5 seconds.
	MasterCount int

	// ServiceResolver is used in CR webhook converters to resolve webhook's service names
	ServiceResolver webhook.ServiceResolver
	// AuthResolverWrapper is used in CR webhook converters
	AuthResolverWrapper webhook.AuthenticationInfoResolverWrapper
}

type Config struct {
	GenericConfig *genericapiserver.RecommendedConfig
	ExtraConfig   ExtraConfig
}

type completedConfig struct {
	GenericConfig genericapiserver.CompletedConfig
	ExtraConfig   *ExtraConfig
}

type CompletedConfig struct {
	// Embed a private pointer that cannot be instantiated outside of this package.
	*completedConfig
}

type CustomResourceDefinitions struct {
	GenericAPIServer *genericapiserver.GenericAPIServer

	// provided for easier embedding
	Informers internalinformers.SharedInformerFactory
}

// Complete fills in any fields not set that are required to have valid data. It's mutating the receiver.
func (cfg *Config) Complete() CompletedConfig {
	c := completedConfig{
		cfg.GenericConfig.Complete(),
		&cfg.ExtraConfig,
	}

	c.GenericConfig.EnableDiscovery = false
	c.GenericConfig.Version = &version.Info{
		Major: "0",
		Minor: "1",
	}

	return CompletedConfig{&c}
}

// New returns a new instance of CustomResourceDefinitions from the given config.
func (c completedConfig) New(delegationTarget genericapiserver.DelegationTarget) (*CustomResourceDefinitions, error) {
	// 1. 创建通用的 genericServer,名称为 apiextensions-apiserver
	genericServer, err := c.GenericConfig.New("apiextensions-apiserver", delegationTarget)
	if err != nil {
		return nil, err
	}

	// 2. 封装 GenericAPIServer 为 CustomResourceDefinitions
	s := &CustomResourceDefinitions{
		GenericAPIServer: genericServer,
	}

	// 3.1 初始化 apiGroupInfo
	apiResourceConfig := c.GenericConfig.MergedResourceConfig
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(apiextensions.GroupName, Scheme, metav1.ParameterCodec, Codecs)
	if apiResourceConfig.VersionEnabled(v1beta1.SchemeGroupVersion) {
		storage := map[string]rest.Storage{}
		// customresourcedefinitions
		customResourceDefintionStorage := customresourcedefinition.NewREST(Scheme, c.GenericConfig.RESTOptionsGetter)
		storage["customresourcedefinitions"] = customResourceDefintionStorage
		storage["customresourcedefinitions/status"] = customresourcedefinition.NewStatusREST(Scheme, customResourceDefintionStorage)

		apiGroupInfo.VersionedResourcesStorageMap[v1beta1.SchemeGroupVersion.Version] = storage
	}
	if apiResourceConfig.VersionEnabled(v1.SchemeGroupVersion) {
		storage := map[string]rest.Storage{}
		// customresourcedefinitions
		customResourceDefintionStorage := customresourcedefinition.NewREST(Scheme, c.GenericConfig.RESTOptionsGetter)
		storage["customresourcedefinitions"] = customResourceDefintionStorage
		storage["customresourcedefinitions/status"] = customresourcedefinition.NewStatusREST(Scheme, customResourceDefintionStorage)

		apiGroupInfo.VersionedResourcesStorageMap[v1.SchemeGroupVersion.Version] = storage
	}

	// 3.2 根据 apiGroupInfo 在 api 中公开给定的 groupversionresource
	if err := s.GenericAPIServer.InstallAPIGroup(&apiGroupInfo); err != nil {
		return nil, err
	}

	// 4. 初始化 crdClient,并为 crd 配置 SharedInformer 工厂
	crdClient, err := internalclientset.NewForConfig(s.GenericAPIServer.LoopbackClientConfig)
	if err != nil {
		// it's really bad that this is leaking here, but until we can fix the test (which I'm pretty sure isn't even testing what it wants to test),
		// we need to be able to move forward
		return nil, fmt.Errorf("failed to create clientset: %v", err)
	}
	s.Informers = internalinformers.NewSharedInformerFactory(crdClient, 5*time.Minute)

	delegateHandler := delegationTarget.UnprotectedHandler()
	if delegateHandler == nil {
		delegateHandler = http.NotFoundHandler()
	}

	versionDiscoveryHandler := &versionDiscoveryHandler{
		discovery: map[schema.GroupVersion]*discovery.APIVersionHandler{},
		delegate:  delegateHandler,
	}
	groupDiscoveryHandler := &groupDiscoveryHandler{
		discovery: map[string]*discovery.APIGroupHandler{},
		delegate:  delegateHandler,
	}
	// 5. 构建 establishingController,用于控制如何以及何时建立CRD.
	//    构建 crdHandler,它服务于 "/apis" endpoint,用于处理 CRD 资源的更新
	establishingController := establish.NewEstablishingController(s.Informers.Apiextensions().InternalVersion().CustomResourceDefinitions(), crdClient.Apiextensions())
	crdHandler, err := NewCustomResourceDefinitionHandler(
		versionDiscoveryHandler,
		groupDiscoveryHandler,
		s.Informers.Apiextensions().InternalVersion().CustomResourceDefinitions(),
		delegateHandler,
		c.ExtraConfig.CRDRESTOptionsGetter,
		c.GenericConfig.AdmissionControl,
		establishingController,
		c.ExtraConfig.ServiceResolver,
		c.ExtraConfig.AuthResolverWrapper,
		c.ExtraConfig.MasterCount,
		s.GenericAPIServer.Authorizer,
		c.GenericConfig.RequestTimeout,
		time.Duration(c.GenericConfig.MinRequestTimeout)*time.Second,
		apiGroupInfo.StaticOpenAPISpec,
		c.GenericConfig.MaxRequestBodyBytes,
	)
	if err != nil {
		return nil, err
	}
	s.GenericAPIServer.Handler.NonGoRestfulMux.Handle("/apis", crdHandler)
	s.GenericAPIServer.Handler.NonGoRestfulMux.HandlePrefix("/apis/", crdHandler)

	// 6. 定义 controller,后续会在 apiserver 的 "start-apiextensions-controllers" PostStartHook 钩子函数中启动这些 controller
	crdController := NewDiscoveryController(s.Informers.Apiextensions().InternalVersion().CustomResourceDefinitions(), versionDiscoveryHandler, groupDiscoveryHandler)
	namingController := status.NewNamingConditionController(s.Informers.Apiextensions().InternalVersion().CustomResourceDefinitions(), crdClient.Apiextensions())
	nonStructuralSchemaController := nonstructuralschema.NewConditionController(s.Informers.Apiextensions().InternalVersion().CustomResourceDefinitions(), crdClient.Apiextensions())
	apiApprovalController := apiapproval.NewKubernetesAPIApprovalPolicyConformantConditionController(s.Informers.Apiextensions().InternalVersion().CustomResourceDefinitions(), crdClient.Apiextensions())
	finalizingController := finalizer.NewCRDFinalizer(
		s.Informers.Apiextensions().InternalVersion().CustomResourceDefinitions(),
		crdClient.Apiextensions(),
		crdHandler,
	)
	var openapiController *openapicontroller.Controller
	if utilfeature.DefaultFeatureGate.Enabled(apiextensionsfeatures.CustomResourcePublishOpenAPI) {
		openapiController = openapicontroller.NewController(s.Informers.Apiextensions().InternalVersion().CustomResourceDefinitions())
	}

	// 7. 对 GenericAPIServer 添加 PostStartHook,用于启动 SharedInformerFactory, controller,并确保 SharedInformer 队列中的 crd 已经同步
	s.GenericAPIServer.AddPostStartHookOrDie("start-apiextensions-informers", func(context genericapiserver.PostStartHookContext) error {
		s.Informers.Start(context.StopCh)
		return nil
	})
	s.GenericAPIServer.AddPostStartHookOrDie("start-apiextensions-controllers", func(context genericapiserver.PostStartHookContext) error {
		// OpenAPIVersionedService and StaticOpenAPISpec are populated in generic apiserver PrepareRun().
		// Together they serve the /openapi/v2 endpoint on a generic apiserver. A generic apiserver may
		// choose to not enable OpenAPI by having null openAPIConfig, and thus OpenAPIVersionedService
		// and StaticOpenAPISpec are both null. In that case we don't run the CRD OpenAPI controller.
		if utilfeature.DefaultFeatureGate.Enabled(apiextensionsfeatures.CustomResourcePublishOpenAPI) && s.GenericAPIServer.OpenAPIVersionedService != nil && s.GenericAPIServer.StaticOpenAPISpec != nil {
			go openapiController.Run(s.GenericAPIServer.StaticOpenAPISpec, s.GenericAPIServer.OpenAPIVersionedService, context.StopCh)
		}

		// 负责监控 CRD 对象的变化并动态创建 API Server 中对应的 API 路径和资源对象.
		// 当用户创建或修改 CRD 时, crdController 将根据 CRD 对象中的定义动态创建相应的 API 路径和资源对象,以便用户可以通过 API Server 访问和操作这些资源对象
		go crdController.Run(context.StopCh)
		// 负责为 CRD 对象自动生成名称. 当用户创建 CRD 时,namingController 将根据 CRD 对象中的定义自动生成名称.这样可以避免用户手动命名造成的错误,并确保 CRD 对象的名称唯一
		go namingController.Run(context.StopCh)
		// 负责管理 CRD 对象的 establisher 字段
		// 它会确保这些字段始终准确地反映 CRD 对象所属的 API 扩展程序,并避免因为 establisher 字段不正确而导致的一些问题
		go establishingController.Run(context.StopCh)
		// 负责验证用户定义的 CRD 对象中的非结构化字段
		// 当用户定义 CRD 对象时,如果其中包含非结构化字段,nonStructuralSchemaController 将负责验证这些字段的格式和内容,以确保它们符合 Kubernetes API Server 的要求
		go nonStructuralSchemaController.Run(5, context.StopCh)
		// 负责管理 CRD 对象的审核流程
		// 当用户提交 CRD 对象时,apiApprovalController 将负责审核并批准这些对象.这样可以确保用户创建的 CRD 对象符合 Kubernetes API Server 的规范,并且不会对集群的稳定性造成影响
		go apiApprovalController.Run(5, context.StopCh)
		// 负责处理 CRD 对象的终止流程
		//  当用户删除 CRD 对象时,finalizingController 将负责处理对象的终止流程,包括清理与该对象相关的资源和元数据.这样可以确保 CRD 对象的删除操作能够顺利完成,避免产生不必要的资源泄漏和数据残留
		go finalizingController.Run(5, context.StopCh)
		return nil
	})
	// we don't want to report healthy until we can handle all CRDs that have already been registered.  Waiting for the informer
	// to sync makes sure that the lister will be valid before we begin.  There may still be races for CRDs added after startup,
	// but we won't go healthy until we can handle the ones already present.
	s.GenericAPIServer.AddPostStartHookOrDie("crd-informer-synced", func(context genericapiserver.PostStartHookContext) error {
		return wait.PollImmediateUntil(100*time.Millisecond, func() (bool, error) {
			return s.Informers.Apiextensions().InternalVersion().CustomResourceDefinitions().Informer().HasSynced(), nil
		}, context.StopCh)
	})

	return s, nil
}

func DefaultAPIResourceConfigSource() *serverstorage.ResourceConfig {
	ret := serverstorage.NewResourceConfig()
	// NOTE: GroupVersions listed here will be enabled by default. Don't put alpha versions in the list.
	ret.EnableVersions(
		v1beta1.SchemeGroupVersion,
		v1.SchemeGroupVersion,
	)

	return ret
}
