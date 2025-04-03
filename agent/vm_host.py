from __future__ import annotations

import json
import os
import shutil
import tempfile
from datetime import datetime
from typing import Any, dict

from agent.job import job, step
from agent.server import Server


class VMHost(Server):
    """
    Class for managing a VM host server that can run virtual machines
    """
    def __init__(self, directory=None):
        super().__init__(directory)
        self.vm_directory = os.path.join(self.directory, "vms")
        self.vm_config_directory = os.path.join(self.directory, "vm_configs")
        self.vm_images_directory = os.path.join(self.directory, "vm_images")
        self.vm_templates_directory = os.path.join(self.directory, "vm_templates")
        # Create required directories if they don't exist
        for directory in [
            self.vm_directory,
            self.vm_config_directory,
            self.vm_images_directory,
            self.vm_templates_directory
        ]:
            os.makedirs(directory, exist_ok=True)

    @job("Setup VM Host")
    def setup_vm_host(self, config: dict[str, Any]):
        """
        Set up the VM host with required dependencies and configurations
        """
        self._install_dependencies_step()
        self._configure_libvirt_step(config)
        self._configure_networking_step(config)
        self._setup_storage_step(config)
        return {"status": "success", "message": "VM host setup completed"}

    @step("Install Dependencies")
    def _install_dependencies_step(self):
        """Install required dependencies for VM management"""
        packages = [
            "qemu-kvm",
            "libvirt-daemon-system",
            "libvirt-clients",
            "bridge-utils",
            "virtinst",
            "virt-top",
            "libguestfs-tools",
            "qemu-utils",
            "python3-libvirt",
            "python3-lxml",
        ]
        install_cmd = f"apt-get update && apt-get install -y {' '.join(packages)}"
        return self.execute(install_cmd)

    @step("Configure libvirt")
    def _configure_libvirt_step(self, config: dict[str, Any]):
        """Configure libvirt daemon and settings"""
        try:
            # Enable and start libvirtd
            self.execute("systemctl enable libvirtd")
            self.execute("systemctl start libvirtd")
            # Ensure libvirt is running
            result = self.execute("systemctl status libvirtd")
            if "active (running)" not in result["output"]:
                raise Exception("Failed to start libvirtd service")
            # Configure user permissions if specified
            if user := config.get("user"):
                self.execute(f"usermod -aG libvirt {user}")
                self.execute(f"usermod -aG kvm {user}")
            return {"status": "success", "message": "libvirt configured successfully"}
        except Exception as e:
            return {"status": "error", "message": f"{e!s}"}

    @step("Configure Networking")
    def _configure_networking_step(self, config: dict[str, Any]):
        """Configure networking for VMs"""
        try:
            # Create default network if it doesn't exist
            check_default = self.execute("virsh net-list --all | grep default")
            if "default" not in check_default["output"]:
                self.execute("virsh net-define /etc/libvirt/qemu/networks/default.xml")
                self.execute("virsh net-autostart default")
                self.execute("virsh net-start default")
            # Create bridge network if configured
            if bridge_config := config.get("bridge_network"):
                bridge_name = bridge_config.get("name", "br0")
                bridge_ip = bridge_config.get("ip", "192.168.100.1")
                bridge_netmask = bridge_config.get("netmask", "255.255.255.0")
                bridge_dhcp_start = bridge_config.get("dhcp_start", "192.168.100.100")
                bridge_dhcp_end = bridge_config.get("dhcp_end", "192.168.100.200")
                # Create bridge network XML
                bridge_xml = f"""
                <network>
                  <name>{bridge_name}</name>
                  <forward mode="nat"/>
                  <bridge name="{bridge_name}" stp="on" delay="0"/>
                  <ip address="{bridge_ip}" netmask="{bridge_netmask}">
                    <dhcp>
                      <range start="{bridge_dhcp_start}" end="{bridge_dhcp_end}"/>
                    </dhcp>
                  </ip>
                </network>
                """
                with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
                    temp_file.write(bridge_xml)
                    temp_file_path = temp_file.name
                self.execute(f"virsh net-define {temp_file_path}")
                self.execute(f"virsh net-autostart {bridge_name}")
                self.execute(f"virsh net-start {bridge_name}")
                os.unlink(temp_file_path)
            return {"status": "success", "message": "Networking configured successfully"}
        except Exception as e:
            return {"status": "error", "message": f"{e!s}"}

    @step("Setup Storage")
    def _setup_storage_step(self, config: dict[str, Any]):
        """Configure storage pools for VMs"""
        try:
            # Create default storage pool if it doesn't exist
            check_default = self.execute("virsh pool-list --all | grep default")
            if "default" not in check_default["output"]:
                # Create directory for default pool
                default_path = config.get("storage_path", "/var/lib/libvirt/images")
                os.makedirs(default_path, exist_ok=True)
                # Define default pool
                pool_xml = f"""
                <pool type="dir">
                  <name>default</name>
                  <target>
                    <path>{default_path}</path>
                  </target>
                </pool>
                """
                with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
                    temp_file.write(pool_xml)
                    temp_file_path = temp_file.name
                self.execute(f"virsh pool-define {temp_file_path}")
                self.execute("virsh pool-build default")
                self.execute("virsh pool-autostart default")
                self.execute("virsh pool-start default")
                os.unlink(temp_file_path)
            return {"status": "success", "message": "Storage configured successfully"}
        except Exception as e:
            return {"status": "error", "message": f"{e!s}"}

    @job("Create VM")
    def create_vm(self, vm_config: dict[str, Any]):
        """
        Create a new virtual machine based on the provided configuration
        """
        vm_name = vm_config.get("name")
        if not vm_name:
            raise ValueError("VM name is required")
        # Prepare VM storage and config
        self._prepare_vm_storage_step(vm_name, vm_config)
        # Create VM with cloud-init if provided
        if cloud_init_config := vm_config.get("cloud_init"):
            self._create_vm_with_cloud_init_step(vm_name, vm_config, cloud_init_config)
        else:
            self._create_vm_step(vm_name, vm_config)
        # Start VM if required
        if vm_config.get("start", True):
            self._start_vm_step(vm_name)
        return {"status": "success", "message": f"VM {vm_name} created successfully"}

    @step("Prepare VM Storage")
    def _prepare_vm_storage_step(self, vm_name: str, vm_config: dict[str, Any]):
        """Create disk image for VM"""
        try:
            disk_size = vm_config.get("disk", 20)
            disk_format = vm_config.get("disk_format", "qcow2")
            disk_path = os.path.join(self.vm_directory, f"{vm_name}.{disk_format}")
            # Check if image template is provided
            if image_template := vm_config.get("image_template"):
                # Clone from template
                template_path = os.path.join(self.vm_images_directory, f"{image_template}.{disk_format}")
                if not os.path.exists(template_path):
                    raise FileNotFoundError(f"Template image {template_path} not found")
                self.execute(f"qemu-img create -f {disk_format} -b {template_path} {disk_path}")
                # Resize if necessary
                if disk_size:
                    self.execute(f"qemu-img resize {disk_path} {disk_size}G")
            else:
                # Create new disk
                self.execute(f"qemu-img create -f {disk_format} {disk_path} {disk_size}G")
            return {"status": "success", "message": "VM storage prepared", "disk_path": disk_path}
        except Exception as e:
            return {"status": "error", "message": f"{e!s}"}

    @step("Create VM with Cloud-Init")
    def _create_vm_with_cloud_init_step(
        self,
        vm_name: str,
        vm_config: dict[str, Any],
        cloud_init_config: dict[str, Any]
    ):
        """Create VM with cloud-init configuration"""
        try:
            # Create cloud-init ISO
            iso_path = self._create_cloud_init_iso(vm_name, cloud_init_config)
            # Create VM config
            cpu = vm_config.get("cpu", 1)
            memory = vm_config.get("memory", 1024)
            disk_format = vm_config.get("disk_format", "qcow2")
            disk_path = os.path.join(self.vm_directory, f"{vm_name}.{disk_format}")
            network = vm_config.get("network", {"type": "default"})
            # Determine network configuration
            network_type = network.get("type", "default")
            network_opts = f"--network network={network_type}"
            if mac_address := network.get("mac_address"):
                network_opts += f",mac={mac_address}"
            # Build virt-install command
            cmd = (
                f"virt-install --name {vm_name} "
                f"--vcpus {cpu} --memory {memory} "
                f"--disk {disk_path},format={disk_format} "
                f"--disk {iso_path},device=cdrom "
                f"{network_opts} "
                "--os-variant ubuntu20.04 "  # This can be configurable based on the OS
                "--graphics none "
                "--noautoconsole "
                "--import"
            )
            result = self.execute(cmd)
            return {"status": "success", "message": "VM created with cloud-init", "output": result["output"]}
        except Exception as e:
            return {"status": "error", "message": f"{e!s}"}

    def _create_cloud_init_iso(self, vm_name: str, cloud_init_config: dict[str, Any]) -> str:
        """Create cloud-init ISO for VM"""
        # Create temporary directory for cloud-init files
        temp_dir = tempfile.mkdtemp()
        try:
            # Write user-data
            if user_data := cloud_init_config.get("user_data"):
                with open(os.path.join(temp_dir, "user-data"), "w") as f:
                    f.write(user_data)
            else:
                # Create default user-data
                with open(os.path.join(temp_dir, "user-data"), "w") as f:
                    f.write("#cloud-config\n")
                    yaml_data = {
                        "hostname": vm_name,
                        "preserve_hostname": False,
                        "ssh_pwauth": True,
                        "chpasswd": {
                            "expire": False,
                            "list": ["ubuntu:ubuntu"]
                        },
                        "users": [
                            {
                                "name": "ubuntu",
                                "sudo": "ALL=(ALL) NOPASSWD:ALL",
                                "shell": "/bin/bash"
                            }
                        ]
                    }
                    # Add SSH keys if provided
                    if ssh_keys := cloud_init_config.get("ssh_authorized_keys"):
                        yaml_data["ssh_authorized_keys"] = ssh_keys
                    f.write(json.dumps(yaml_data))
            # Write meta-data
            if meta_data := cloud_init_config.get("meta_data"):
                with open(os.path.join(temp_dir, "meta-data"), "w") as f:
                    f.write(meta_data)
            else:
                # Create default meta-data
                with open(os.path.join(temp_dir, "meta-data"), "w") as f:
                    f.write(f"instance-id: {vm_name}\n")
                    f.write(f"local-hostname: {vm_name}\n")
            # Write network-config if provided
            if network_config := cloud_init_config.get("network_config"):
                with open(os.path.join(temp_dir, "network-config"), "w") as f:
                    f.write(network_config)
            # Create ISO file
            iso_path = os.path.join(self.vm_directory, f"{vm_name}-cloudinit.iso")
            cmd = (
                f"genisoimage -output {iso_path} -volid cidata -joliet -rock {temp_dir}/user-data "
                f"{temp_dir}/meta-data" + (f" {temp_dir}/network-config" if network_config else "")
            )
            self.execute(cmd)
            return iso_path
        finally:
            # Clean up temporary directory
            shutil.rmtree(temp_dir)

    @step("Create VM")
    def _create_vm_step(self, vm_name: str, vm_config: dict[str, Any]):
        """Create VM without cloud-init"""
        try:
            # Create VM config
            cpu = vm_config.get("cpu", 1)
            memory = vm_config.get("memory", 1024)
            disk_format = vm_config.get("disk_format", "qcow2")
            disk_path = os.path.join(self.vm_directory, f"{vm_name}.{disk_format}")
            network = vm_config.get("network", {"type": "default"})
            # Determine network configuration
            network_type = network.get("type", "default")
            network_opts = f"--network network={network_type}"
            if mac_address := network.get("mac_address"):
                network_opts += f",mac={mac_address}"
            # Build virt-install command
            cmd = (
                f"virt-install --name {vm_name} "
                f"--vcpus {cpu} --memory {memory} "
                f"--disk {disk_path},format={disk_format} "
                f"{network_opts} "
                "--os-variant ubuntu20.04 "  # This can be configurable based on the OS
                "--graphics none "
                "--noautoconsole "
                "--boot hd"
            )
            result = self.execute(cmd)
            return {"status": "success", "message": "VM created", "output": result["output"]}
        except Exception as e:
            return {"status": "error", "message": f"{e!s}"}

    @step("Start VM")
    def _start_vm_step(self, vm_name: str):
        """Start a VM"""
        try:
            # Check if VM exists
            check_vm = self.execute(f"virsh dominfo {vm_name}")
            if "error" in check_vm["output"].lower():
                raise Exception(f"VM {vm_name} does not exist")
            # Check if VM is already running
            if "running" in check_vm["output"].lower():
                return {"status": "success", "message": f"VM {vm_name} is already running"}
            # Start VM
            result = self.execute(f"virsh start {vm_name}")
            return {"status": "success", "message": f"VM {vm_name} started", "output": result["output"]}
        except Exception as e:
            return {"status": "error", "message": f"{e!s}"}

    @job("Start VM")
    def start_vm(self, vm_name: str):
        """Start a virtual machine"""
        return self._start_vm_step(vm_name)

    @job("Stop VM")
    def stop_vm(self, vm_name: str, force: bool = False):
        """Stop a virtual machine"""
        try:
            # Check if VM exists
            check_vm = self.execute(f"virsh dominfo {vm_name}")
            if "error" in check_vm["output"].lower():
                raise Exception(f"VM {vm_name} does not exist")
            # Check if VM is already stopped
            if "shut off" in check_vm["output"].lower():
                return {"status": "success", "message": f"VM {vm_name} is already stopped"}
            # Stop VM (shutdown or destroy)
            if force:
                result = self.execute(f"virsh destroy {vm_name}")
            else:
                result = self.execute(f"virsh shutdown {vm_name}")
            msg = "forcefully terminated" if force else "shutdown initiated"
            return {
                "status": "success",
                "message": f"VM {vm_name} {msg}",
                "output": result["output"]
            }
        except Exception as e:
            return {"status": "error", "message": f"{e!s}"}

    def _stop_running_vm(self, vm_name: str):
        """Stop a running VM"""
        check_vm = self.execute(f"virsh dominfo {vm_name}")
        if "running" in check_vm["output"].lower():
            self.execute(f"virsh destroy {vm_name}")

    def _delete_vm_storage(self, vm_name: str):
        """Delete VM storage files"""
        vm_disk = os.path.join(self.vm_directory, f"{vm_name}.qcow2")
        if os.path.exists(vm_disk):
            os.remove(vm_disk)

    @job("Delete VM")
    def delete_vm(self, vm_name: str, delete_storage: bool = True):
        """Delete a virtual machine"""
        try:
            # Check if VM exists
            check_vm = self.execute(f"virsh dominfo {vm_name}")
            if "error" in check_vm["output"].lower():
                raise Exception(f"VM {vm_name} does not exist")

            # Stop if running and delete VM
            self._stop_running_vm(vm_name)
            result = self.execute(f"virsh undefine {vm_name}")

            # Delete storage if requested
            if delete_storage:
                self._delete_vm_storage(vm_name)

            return {"status": "success", "message": f"VM {vm_name} deleted", "output": result["output"]}

        except Exception as e:
            return {"status": "error", "message": f"{e!s}"}

    @job("Get VM Status")
    def get_vm_status(self, vm_name: str | None = None):
        """Get status of one or all VMs"""
        try:
            # Get single VM status
            if vm_name:
                result = self.execute(f"virsh dominfo {vm_name}")
                if "error" in result["output"].lower():
                    return {"status": "error", "message": f"VM {vm_name} not found"}

                # Parse domain info
                status_info = self._parse_domain_info(result["output"])

                # Get network info
                status_info["interfaces"] = self._get_network_info(vm_name)

                return {"status": "success", "vm": vm_name, "info": status_info}

            # Get all VMs status
            result = self.execute("virsh list --all")
            vms = []
            for line in result["output"].split("\n")[2:]:
                parts = line.strip().split()
                if len(parts) >= 3:
                    vms.append({
                        "id": parts[0],
                        "name": parts[1],
                        "state": " ".join(parts[2:])
                    })
            return {"status": "success", "vms": vms}

        except Exception as e:
            return {"status": "error", "message": f"{e!s}"}

    def _parse_domain_info(self, output: str) -> dict:
        """Parse virsh dominfo output into a dictionary"""
        status_info = {}
        for line in output.split("\n"):
            if ":" in line:
                key, value = line.split(":", 1)
                status_info[key.strip()] = value.strip()
        return status_info

    def _get_network_info(self, vm_name: str) -> list:
        """Get network interface information for a VM"""
        try:
            network_info = self.execute(f"virsh domifaddr {vm_name}")
            interfaces = []
            for line in network_info["output"].split("\n")[2:]:
                parts = line.strip().split()
                if len(parts) >= 4:
                    interfaces.append({
                        "name": parts[0],
                        "mac": parts[1],
                        "protocol": parts[2],
                        "address": parts[3]
                    })
            return interfaces
        except Exception:
            return []

    @job("Create VM Snapshot")
    def create_vm_snapshot(self, vm_name: str, snapshot_name: str | None = None):
        """Create a snapshot of a virtual machine"""
        try:
            # Generate snapshot name if not provided
            if not snapshot_name:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                snapshot_name = f"{vm_name}_snapshot_{timestamp}"
            # Check if VM exists
            check_vm = self.execute(f"virsh dominfo {vm_name}")
            if "error" in check_vm["output"].lower():
                raise Exception(f"VM {vm_name} does not exist")
            # Create snapshot
            snapshot_xml = f"""
            <domainsnapshot>
              <name>{snapshot_name}</name>
              <description>Snapshot created at {datetime.now().isoformat()}</description>
            </domainsnapshot>
            """
            with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
                temp_file.write(snapshot_xml)
                temp_file_path = temp_file.name
            result = self.execute(f"virsh snapshot-create {vm_name} {temp_file_path}")
            os.unlink(temp_file_path)
            return {
                "status": "success",
                "message": f"Snapshot {snapshot_name} created for VM {vm_name}",
                "output": result["output"]
            }
        except Exception as e:
            return {"status": "error", "message": f"{e!s}"}

    @job("Restore VM Snapshot")
    def restore_vm_snapshot(self, vm_name: str, snapshot_name: str):
        """Restore a virtual machine from a snapshot"""
        try:
            # Check if VM exists
            check_vm = self.execute(f"virsh dominfo {vm_name}")
            if "error" in check_vm["output"].lower():
                raise Exception(f"VM {vm_name} does not exist")
            # Check if snapshot exists
            check_snapshot = self.execute(f"virsh snapshot-list {vm_name} | grep {snapshot_name}")
            if snapshot_name not in check_snapshot["output"]:
                raise Exception(f"Snapshot {snapshot_name} does not exist for VM {vm_name}")
            # Restore snapshot
            result = self.execute(f"virsh snapshot-revert {vm_name} {snapshot_name}")
            return {
                "status": "success",
                "message": f"VM {vm_name} restored to snapshot {snapshot_name}",
                "output": result["output"]
            }
        except Exception as e:
            return {"status": "error", "message": f"{e!s}"}

    @job("Get VM Console")
    def get_vm_console(self, vm_name: str):
        """Get console URL for a virtual machine"""
        try:
            # Check if VM exists
            check_vm = self.execute(f"virsh dominfo {vm_name}")
            if "error" in check_vm["output"].lower():
                raise Exception(f"VM {vm_name} does not exist")
            # Get console information
            result = self.execute(f"virsh domdisplay {vm_name}")
            console_url = result["output"].strip()
            return {"status": "success", "vm": vm_name, "console_url": console_url}
        except Exception as e:
            return {"status": "error", "message": f"{e!s}"}
