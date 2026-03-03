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
 */

package vgpuhook

import (
	"fmt"

	v1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/log"
	"libvirt.org/go/libvirtxml"
)

// VGPUDedicatedHook mutates the mdev uuid for the target's domain XML in vGPU live migrations
func VGPUDedicatedHook(vmi *v1.VirtualMachineInstance, domain *libvirtxml.Domain) error {
	mdevUUID, ok := vmi.Annotations[v1.TargetMdevUUIDAnnotation]
	if !ok {
		return fmt.Errorf("missing VMI annotation target-mdev-uuid")
	}

	// need to check for type=mdev so we don't try to migrate a passthrough GPU
	if len(domain.Devices.Hostdevs) == 1 && domain.Devices.Hostdevs[0].SubsysMDev != nil {
		domain.Devices.Hostdevs[0].SubsysMDev.Source.Address.UUID = mdevUUID
	} else {
		return fmt.Errorf("failed to retrieve mDev vGPU from domain")
	}

	log.Log.Object(vmi).Info("vGPU-hook: mDev uuid mutation completed")
	return nil
}
