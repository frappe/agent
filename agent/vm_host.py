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
            self.vm_templates_directory,
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
                with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
                    temp_file.write(bridge_xml)
                    temp_file_path = temp_file.name
                self.execute(f"virsh net-define {temp_file_path}")
                self.execute(f"virsh net-autostart {bridge_name}")
                self.execute(f"virsh net-start {bridge_name}")
                os.unlink(temp_file_path)
            return {
                "status": "success",
                "message": "Networking configured successfully",
            }
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
                with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
                    temp_file.write(pool_xml)
                    temp_file_path = temp_file.name
                self.execute(f"virsh pool-define {temp_file_path}")
                self.execute("virsh pool-build default")
                self.execute("virsh pool-autostart default")
                self.execute("virsh pool-start default")
                os.unlink(temp_file_path)
                
            # Setup NFS storage pool if configured
            if nfs_config := config.get("nfs_storage"):
                nfs_server = nfs_config.get("server")
                nfs_export = nfs_config.get("export", "/exports/vm_storage")
                nfs_pool_name = nfs_config.get("pool_name", "nfs_storage")
                
                # Check if NFS pool already exists
                check_nfs = self.execute(f"virsh pool-list --all | grep {nfs_pool_name}")
                if nfs_pool_name not in check_nfs["output"]:
                    # Define NFS pool
                    nfs_pool_xml = f"""
                    <pool type="netfs">
                      <name>{nfs_pool_name}</name>
                      <source>
                        <host name="{nfs_server}"/>
                        <dir path="{nfs_export}"/>
                        <format type="auto"/>
                      </source>
                      <target>
                        <path>{config.get("nfs_mount_point", "/mnt/vm_storage")}</path>
                      </target>
                    </pool>
                    """
                    with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
                        temp_file.write(nfs_pool_xml)
                        temp_file_path = temp_file.name
                    self.execute(f"virsh pool-define {temp_file_path}")
                    self.execute(f"virsh pool-build {nfs_pool_name}")
                    self.execute(f"virsh pool-autostart {nfs_pool_name}")
                    self.execute(f"virsh pool-start {nfs_pool_name}")
                    os.unlink(temp_file_path)
                    
                # Verify NFS mount and connectivity
                self._verify_nfs_connectivity(nfs_server, nfs_export, config.get("nfs_mount_point", "/mnt/vm_storage"))
                
            return {"status": "success", "message": "Storage configured successfully"}
        except Exception as e:
            return {"status": "error", "message": f"{e!s}"}
    
    def _verify_nfs_connectivity(self, nfs_server, nfs_export, mount_point):
        """Verify NFS mount and connectivity"""
        try:
            # Check if NFS client is installed
            self.execute("command -v showmount || apt-get install -y nfs-common")
            
            # Check NFS server availability
            result = self.execute(f"showmount -e {nfs_server}", raise_error=False)
            if result["status"] != 0:
                raise Exception(f"Cannot connect to NFS server {nfs_server}: {result['output']}")
                
            # Check if export is available
            if nfs_export not in result["output"]:
                raise Exception(f"NFS export {nfs_export} not found on server {nfs_server}")
                
            # Check mount point
            mount_result = self.execute(f"mountpoint -q {mount_point}", raise_error=False)
            if mount_result["status"] != 0:
                # Try mounting if not mounted
                self.execute(f"mount -t nfs {nfs_server}:{nfs_export} {mount_point}")
                
            # Check write access
            test_file = f"{mount_point}/nfs_test_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            self.execute(f"touch {test_file} && rm {test_file}")
            
            return {"status": "success", "message": "NFS connectivity verified"}
        except Exception as e:
            return {"status": "error", "message": f"NFS verification failed: {e!s}"}

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
            return {
                "status": "success",
                "message": "VM storage prepared",
                "disk_path": disk_path,
            }
        except Exception as e:
            return {"status": "error", "message": f"{e!s}"}

    @step("Create VM with Cloud-Init")
    def _create_vm_with_cloud_init_step(
        self, vm_name: str, vm_config: dict[str, Any], cloud_init_config: dict[str, Any]
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
            return {
                "status": "success",
                "message": "VM created with cloud-init",
                "output": result["output"],
            }
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
                        "chpasswd": {"expire": False, "list": ["ubuntu:ubuntu"]},
                        "users": [
                            {
                                "name": "ubuntu",
                                "sudo": "ALL=(ALL) NOPASSWD:ALL",
                                "shell": "/bin/bash",
                            }
                        ],
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
            return {
                "status": "success",
                "message": "VM created",
                "output": result["output"],
            }
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
                return {
                    "status": "success",
                    "message": f"VM {vm_name} is already running",
                }
            # Start VM
            result = self.execute(f"virsh start {vm_name}")
            return {
                "status": "success",
                "message": f"VM {vm_name} started",
                "output": result["output"],
            }
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
                return {
                    "status": "success",
                    "message": f"VM {vm_name} is already stopped",
                }
            # Stop VM (shutdown or destroy)
            if force:
                result = self.execute(f"virsh destroy {vm_name}")
            else:
                result = self.execute(f"virsh shutdown {vm_name}")
            msg = "forcefully terminated" if force else "shutdown initiated"
            return {
                "status": "success",
                "message": f"VM {vm_name} {msg}",
                "output": result["output"],
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

            return {
                "status": "success",
                "message": f"VM {vm_name} deleted",
                "output": result["output"],
            }

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
                    vms.append({"id": parts[0], "name": parts[1], "state": " ".join(parts[2:])})
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
                    interfaces.append(
                        {
                            "name": parts[0],
                            "mac": parts[1],
                            "protocol": parts[2],
                            "address": parts[3],
                        }
                    )
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
            with tempfile.NamedTemporaryFile(mode="w", delete=False) as temp_file:
                temp_file.write(snapshot_xml)
                temp_file_path = temp_file.name
            result = self.execute(f"virsh snapshot-create {vm_name} {temp_file_path}")
            os.unlink(temp_file_path)
            return {
                "status": "success",
                "message": f"Snapshot {snapshot_name} created for VM {vm_name}",
                "output": result["output"],
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
                "output": result["output"],
            }
        except Exception as e:
            return {"status": "error", "message": f"{e!s}"}

    @job("Get VM Console")
    def get_vm_console(self, vm_name: str):
        """Get VM console URL"""
        try:
            console_info = self.execute(f"virsh domdisplay {vm_name}")
            return {
                "status": "success",
                "console_url": console_info["output"].strip(),
            }
        except Exception as e:
            return {"status": "error", "message": f"{e!s}"}

    @job("Migrate VM")
    def migrate_vm(self, vm_name: str, destination_host: str, live: bool = True, storage_pool: str = None):
        """
        Migrate a VM to another host using shared storage or storage migration
        
        Args:
            vm_name: Name of the VM to migrate
            destination_host: Hostname or IP of the destination host
            live: Whether to perform live migration (no downtime)
            storage_pool: Name of the shared storage pool to use
        """
        # Check if VM exists
        check_vm = self.execute(f"virsh dominfo {vm_name}", raise_error=False)
        if check_vm["status"] != 0:
            return {"status": "error", "message": f"VM {vm_name} not found"}
            
        # Check VM state
        vm_info = self._parse_domain_info(check_vm["output"])
        vm_state = vm_info.get("State", "")
        
        # Prepare migration command
        migrate_cmd = f"virsh migrate {vm_name} qemu+ssh://{destination_host}/system"
        
        # Add options based on parameters
        options = []
        
        if live and "running" in vm_state.lower():
            options.append("--live")
            
        if storage_pool:
            # Check if storage pool exists on both hosts
            check_pool = self.execute(f"virsh pool-info {storage_pool}", raise_error=False)
            if check_pool["status"] != 0:
                return {"status": "error", "message": f"Storage pool {storage_pool} not found on source host"}
                
            # Verify storage pool on destination
            check_dest_pool = self.execute(
                f"ssh {destination_host} virsh pool-info {storage_pool}", 
                raise_error=False
            )
            if check_dest_pool["status"] != 0:
                return {
                    "status": "error", 
                    "message": f"Storage pool {storage_pool} not found on destination host"
                }
                
            # Add storage migration options
            options.append("--persistent")
            options.append("--undefinesource")
            options.append(f"--storage {storage_pool}")
        else:
            # Shared storage migration (requires same paths on both hosts)
            options.append("--persistent")
            options.append("--undefinesource")
            
        # Construct final command
        if options:
            migrate_cmd = f"{migrate_cmd} {' '.join(options)}"
            
        # Execute migration
        try:
            result = self.execute(migrate_cmd)
            return {
                "status": "success",
                "message": f"VM {vm_name} successfully migrated to {destination_host}",
                "output": result["output"]
            }
        except Exception as e:
            return {"status": "error", "message": f"Migration failed: {e!s}"}
            
    @job("Copy VM to NFS Storage")
    def copy_vm_to_nfs_storage(self, vm_name: str, nfs_storage_pool: str):
        """
        Copy a VM to NFS storage to prepare for migration
        
        Args:
            vm_name: Name of the VM to copy
            nfs_storage_pool: Name of the NFS storage pool
        """
        try:
            # Check if VM exists
            check_vm = self.execute(f"virsh dominfo {vm_name}", raise_error=False)
            if check_vm["status"] != 0:
                return {"status": "error", "message": f"VM {vm_name} not found"}
                
            # Check VM state
            vm_info = self._parse_domain_info(check_vm["output"])
            vm_state = vm_info.get("State", "")
            
            # Stop VM if running
            if "running" in vm_state.lower():
                self._stop_running_vm(vm_name)
                
            # Check if NFS storage pool exists
            check_pool = self.execute(f"virsh pool-info {nfs_storage_pool}", raise_error=False)
            if check_pool["status"] != 0:
                return {"status": "error", "message": f"Storage pool {nfs_storage_pool} not found"}
                
            # Get NFS storage pool path
            pool_path = self.execute(f"virsh pool-dumpxml {nfs_storage_pool} | grep -oP '(?<=<path>).*?(?=</path>)'")
            nfs_path = pool_path["output"].strip()
            
            # Get current VM disk path
            disk_path = self.execute(
                f"virsh dumpxml {vm_name} | grep -oP '(?<=<source file=\\\").*?(?=\\\")'")["output"].strip()
            disk_filename = os.path.basename(disk_path)
            
            # Copy disk to NFS storage
            new_disk_path = os.path.join(nfs_path, disk_filename)
            self.execute(f"cp {disk_path} {new_disk_path}")
            
            # Update VM XML to use new disk path
            self.execute(f"virsh dumpxml {vm_name} > /tmp/{vm_name}.xml")
            self.execute(f"sed -i 's|{disk_path}|{new_disk_path}|g' /tmp/{vm_name}.xml")
            
            # Redefine VM with new XML
            self.execute(f"virsh undefine {vm_name}")
            self.execute(f"virsh define /tmp/{vm_name}.xml")
            
            # Start VM if it was running before
            if "running" in vm_state.lower():
                self._start_vm_step(vm_name)
                
            return {
                "status": "success",
                "message": f"VM {vm_name} successfully copied to NFS storage pool {nfs_storage_pool}",
                "new_disk_path": new_disk_path
            }
        except Exception as e:
            return {"status": "error", "message": f"Copy to NFS storage failed: {e!s}"}
            
    @job("Setup NFS Health Check")
    def setup_nfs_health_check(self, nfs_storage_pool: str):
        """
        Setup a periodic health check for NFS storage
        
        Args:
            nfs_storage_pool: Name of the NFS storage pool to monitor
        """
        try:
            # Check if NFS storage pool exists
            check_pool = self.execute(f"virsh pool-info {nfs_storage_pool}", raise_error=False)
            if check_pool["status"] != 0:
                return {"status": "error", "message": f"Storage pool {nfs_storage_pool} not found"}
                
            # Get NFS storage pool path
            pool_path = self.execute(f"virsh pool-dumpxml {nfs_storage_pool} | grep -oP '(?<=<path>).*?(?=</path>)'")
            nfs_path = pool_path["output"].strip()
            
            # Create health check script
            health_check_script = f"""#!/bin/bash
# NFS health check script for {nfs_storage_pool}

NFS_PATH="{nfs_path}"
LOG_FILE="/var/log/nfs_health_check.log"

# Function to log messages
log() {{
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> $LOG_FILE
}}

# Check if mount point exists
if [ ! -d "$NFS_PATH" ]; then
    log "ERROR: NFS path $NFS_PATH does not exist"
    exit 1
fi

# Check if NFS is mounted
if ! mountpoint -q "$NFS_PATH"; then
    log "ERROR: NFS is not mounted at $NFS_PATH, attempting to refresh storage pool"
    virsh pool-destroy {nfs_storage_pool}
    virsh pool-start {nfs_storage_pool}
    sleep 5
    if ! mountpoint -q "$NFS_PATH"; then
        log "ERROR: Failed to remount NFS at $NFS_PATH"
        exit 1
    else
        log "SUCCESS: NFS remounted successfully at $NFS_PATH"
    fi
fi

# Check if mount is stale
if ! timeout 5 ls -la $NFS_PATH &>/dev/null; then
    log "ERROR: NFS mount is stale at $NFS_PATH, attempting to refresh"
    virsh pool-destroy {nfs_storage_pool}
    umount -f $NFS_PATH 2>/dev/null
    virsh pool-start {nfs_storage_pool}
    sleep 5
    if ! timeout 5 ls -la $NFS_PATH &>/dev/null; then
        log "ERROR: Failed to recover stale NFS mount at $NFS_PATH"
        exit 1
    else
        log "SUCCESS: Recovered from stale NFS mount at $NFS_PATH"
    fi
fi

# Check write access
TEST_FILE="$NFS_PATH/nfs_test_$(date '+%Y%m%d%H%M%S')"
if ! touch $TEST_FILE 2>/dev/null; then
    log "ERROR: Cannot write to NFS mount at $NFS_PATH"
    exit 1
fi
rm $TEST_FILE 2>/dev/null

# All checks passed
log "INFO: NFS health check passed for $NFS_PATH"
exit 0
"""
            
            # Write script to file
            script_path = "/usr/local/bin/nfs_health_check.sh"
            with open(script_path, "w") as f:
                f.write(health_check_script)
                
            # Make script executable
            self.execute(f"chmod +x {script_path}")
            
            # Setup cron job to run every 5 minutes
            cron_entry = f"*/5 * * * * {script_path} >/dev/null 2>&1"
            cron_file = "/etc/cron.d/nfs_health_check"
            with open(cron_file, "w") as f:
                f.write(f"{cron_entry}\n")
                
            # Create log file
            self.execute("touch /var/log/nfs_health_check.log")
            self.execute("chmod 644 /var/log/nfs_health_check.log")
            
            return {
                "status": "success",
                "message": f"NFS health check configured for storage pool {nfs_storage_pool}",
                "script_path": script_path,
                "cron_file": cron_file
            }
        except Exception as e:
            return {"status": "error", "message": f"Failed to setup NFS health check: {e!s}"}

    @job("Monitor NFS Performance")
    def monitor_nfs_performance(self, nfs_storage_pool: str, duration: int = 60, interval: int = 1):
        """
        Monitor NFS performance for a specified duration
        
        Args:
            nfs_storage_pool: Name of the NFS storage pool to monitor
            duration: Duration in seconds to monitor
            interval: Interval in seconds between measurements
        """
        try:
            # Check if NFS storage pool exists
            check_pool = self.execute(f"virsh pool-info {nfs_storage_pool}", raise_error=False)
            if check_pool["status"] != 0:
                return {"status": "error", "message": f"Storage pool {nfs_storage_pool} not found"}
                
            # Get NFS storage pool path
            pool_path = self.execute(f"virsh pool-dumpxml {nfs_storage_pool} | grep -oP '(?<=<path>).*?(?=</path>)'")
            nfs_path = pool_path["output"].strip()
            
            # Install required packages if not available
            self.execute("command -v iostat || apt-get install -y sysstat", raise_error=False)
            self.execute("command -v nfsiostat || apt-get install -y nfs-common", raise_error=False)
                
            # Run performance tests
            results = {
                "timestamp": datetime.now().isoformat(),
                "nfs_path": nfs_path,
                "storage_pool": nfs_storage_pool,
                "mount_info": {},
                "io_stats": {},
                "latency_test": {},
                "throughput_test": {},
            }
            
            # Get mount info
            mount_info = self.execute(f"mount | grep {nfs_path}")
            results["mount_info"]["raw"] = mount_info["output"].strip()
            
            # NFS server info
            try:
                nfs_server = mount_info["output"].split("from ")[1].split(" ")[0]
                results["mount_info"]["server"] = nfs_server
            except:
                results["mount_info"]["server"] = "unknown"
            
            # Get NFS IO stats
            try:
                nfs_iostat = self.execute(f"nfsiostat {nfs_path} {interval} {duration // interval}")
                results["io_stats"]["raw"] = nfs_iostat["output"]
                
                # Parse key metrics
                lines = nfs_iostat["output"].splitlines()
                for i, line in enumerate(lines):
                    if "read:" in line:
                        read_ops = lines[i+1].strip()
                        write_ops = lines[i+2].strip()
                        results["io_stats"]["read_ops"] = read_ops
                        results["io_stats"]["write_ops"] = write_ops
            except Exception as e:
                results["io_stats"]["error"] = str(e)
                
            # Test read/write latency with dd
            try:
                # Create test file
                test_file = f"{nfs_path}/nfs_test_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                
                # Write test
                write_test = self.execute(f"dd if=/dev/zero of={test_file} bs=4k count=1000 oflag=dsync 2>&1")
                write_result = write_test["output"].strip()
                if "copied" in write_result:
                    write_speed = write_result.split(",")[-1].strip()
                    results["latency_test"]["write"] = write_speed
                
                # Read test
                read_test = self.execute(f"dd if={test_file} of=/dev/null bs=4k count=1000 iflag=dsync 2>&1")
                read_result = read_test["output"].strip()
                if "copied" in read_result:
                    read_speed = read_result.split(",")[-1].strip()
                    results["latency_test"]["read"] = read_speed
                    
                # Clean up
                self.execute(f"rm {test_file}")
            except Exception as e:
                results["latency_test"]["error"] = str(e)
                
            # Test throughput with dd
            try:
                # Create test file for throughput (larger file, no sync)
                test_file = f"{nfs_path}/nfs_throughput_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                
                # Write throughput test
                write_test = self.execute(f"dd if=/dev/zero of={test_file} bs=64k count=10000 2>&1")
                write_result = write_test["output"].strip()
                if "copied" in write_result:
                    write_speed = write_result.split(",")[-1].strip()
                    results["throughput_test"]["write"] = write_speed
                
                # Read throughput test
                read_test = self.execute(f"dd if={test_file} of=/dev/null bs=64k count=10000 2>&1")
                read_result = read_test["output"].strip()
                if "copied" in read_result:
                    read_speed = read_result.split(",")[-1].strip()
                    results["throughput_test"]["read"] = read_speed
                    
                # Clean up
                self.execute(f"rm {test_file}")
            except Exception as e:
                results["throughput_test"]["error"] = str(e)
                
            return {
                "status": "success",
                "message": f"NFS performance monitoring completed for {nfs_storage_pool}",
                "results": results
            }
        except Exception as e:
            return {"status": "error", "message": f"Failed to monitor NFS performance: {e!s}"}

    @job("Benchmark NFS Storage")
    def benchmark_nfs_storage(self, nfs_storage_pool: str):
        """
        Run comprehensive benchmark tests on NFS storage pool
        
        Args:
            nfs_storage_pool: Name of the NFS storage pool to benchmark
        """
        try:
            # Check if NFS storage pool exists
            check_pool = self.execute(f"virsh pool-info {nfs_storage_pool}", raise_error=False)
            if check_pool["status"] != 0:
                return {"status": "error", "message": f"Storage pool {nfs_storage_pool} not found"}
                
            # Get NFS storage pool path
            pool_path = self.execute(f"virsh pool-dumpxml {nfs_storage_pool} | grep -oP '(?<=<path>).*?(?=</path>)'")
            nfs_path = pool_path["output"].strip()
            
            # Install required packages if not available
            self.execute("command -v fio || apt-get install -y fio", raise_error=False)
            
            # Create test directory
            test_dir = f"{nfs_path}/nfs_benchmark_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            self.execute(f"mkdir -p {test_dir}")
            
            # Prepare FIO job file for benchmarks
            fio_job = f"""
[global]
directory={test_dir}
ioengine=libaio
direct=1
group_reporting=1
time_based=1
runtime=60
size=1G

[sequential-read]
description=Sequential read test
rw=read
stonewall

[sequential-write]
description=Sequential write test
rw=write
stonewall

[random-read-4k]
description=Random read 4k blocks
bs=4k
rw=randread
stonewall

[random-write-4k]
description=Random write 4k blocks
bs=4k
rw=randwrite
stonewall

[mixed-randread-write]
description=Mixed random read/write 70/30
bs=4k
rw=randrw
rwmixread=70
stonewall
"""
            
            # Write job file
            job_file = f"{nfs_path}/nfs_benchmark.fio"
            with open(job_file, "w") as f:
                f.write(fio_job)
                
            # Run FIO benchmark
            benchmark_result = self.execute(f"fio --output-format=json {job_file}")
            results = json.loads(benchmark_result["output"])
            
            # Clean up
            self.execute(f"rm -rf {test_dir} {job_file}")
            
            # Format results
            formatted_results = {
                "timestamp": datetime.now().isoformat(),
                "nfs_path": nfs_path,
                "storage_pool": nfs_storage_pool,
                "tests": {}
            }
            
            for job in results["jobs"]:
                job_name = job["jobname"]
                formatted_results["tests"][job_name] = {
                    "read": {
                        "bw_bytes": job.get("read", {}).get("bw_bytes", 0),
                        "iops": job.get("read", {}).get("iops", 0),
                        "lat_ns": job.get("read", {}).get("lat_ns", {}).get("mean", 0)
                    },
                    "write": {
                        "bw_bytes": job.get("write", {}).get("bw_bytes", 0),
                        "iops": job.get("write", {}).get("iops", 0),
                        "lat_ns": job.get("write", {}).get("lat_ns", {}).get("mean", 0)
                    }
                }
                
            return {
                "status": "success",
                "message": f"NFS benchmark completed for {nfs_storage_pool}",
                "results": formatted_results
            }
        except Exception as e:
            return {"status": "error", "message": f"Failed to benchmark NFS storage: {e!s}"}
