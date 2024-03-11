# Online disk updates on VMs

On Vagrant you update everything with

1. Run `vagrant up`
2. Change Vagrantfile
3. Run `vagrant reload`

`vagrant reload` will stop your machine and start it back up with the new configuration.

We want our environment to feel as close to AWS as possible. Forced restarts are acceptable for CPU/RAM changes, but not for

1. Disk modifications (Resize, Change performance characteristics)
2. Attach/Detach disks

We'll attempt to bypass Vagrant and directly talk to Libvirt, QEMU, KVM to achieve these "online" changes.

## Online Disk Resize

### qemu-img resize

The standard way of doing this is with

```sh
qemu-img resize <file> <size>
```

But qemu-img doesn't like messing with image files that are already being used by a running machine. You will see something like.

```
qemu-img: Could not open '/var/lib/libvirt/images/machine.img': Failed to get "write" lock
Is another process using the image [/var/lib/libvirt/images/machine.img]?
```

`qemu-img info` has the same problem, so you can't even reliably read the file.

```sh
qemu-img info /var/lib/libvirt/images/machine.img
```

You can bypass this with a forced shared lock.

```sh
qemu-img info -U /var/lib/libvirt/images/machine.img
```

```sh
image: /var/lib/libvirt/images/machine.img
file format: qcow2
virtual size: 20 GiB (21474836480 bytes)
disk size: 31.6 MiB
cluster_size: 65536
backing file: /var/lib/libvirt/images/backbone_vagrant_box_image_0_1671079657_box.img
backing file format: qcow2
Format specific information:
    compat: 1.1
    compression type: zlib
    lazy refcounts: false
    refcount bits: 16
    corrupt: false
    extended l2: false
Child node '/file':
    filename: /var/lib/libvirt/images/machine.img
    protocol type: file
    file length: 31.6 MiB (33161216 bytes)
    disk size: 31.6 MiB
```

Unfortunately, `qemu-img resize` doesn't have `-U` like flag.

### qemu-monitor-command block_resize

We can directly talk to a live machine with `qemu-monitor-command`

```sh
virsh qemu-monitor-command --hmp machine block_resize <device> <size>
```

But this fails with

```sh
Error: Cannot find device='<device>' nor node-name=''
```

We can avoid the error by skippng the `hmp` mode.

List the block devices in JSON (equivalent of `virsh qemu-monitor-command --hmp <machine> info block`)

```sh
virsh qemu-monitor-command <machine> '{"execute":"query-block"}' | jq
```

```json
{
  "return": [
    {
      "device": "",
      "inserted": {
        "node-name": "libvirt-2-format",
        "file": "/var/lib/libvirt/images/machine.img"
      },
      "qdev": "/machine/peripheral/ua-box-volume-0/virtio-backend"
    },
    {
      "device": "",
      "inserted": {
        "node-name": "libvirt-1-format",
        "file": "/var/lib/libvirt/images/machine-vdb.raw"
      },
      "qdev": "/machine/peripheral/ua-disk-volume-0/virtio-backend"
    }
  ]
}
```

Since `device` isn't set, we can try with `node-name`.

```sh
virsh qemu-monitor-command f3-aditya '{"execute":"block_resize", "arguments": {"size": 21474836480, "node-name": "libvirt-2-format"}}'
```

References:

- https://www.qemu.org/docs/master/interop/qemu-storage-daemon-qmp-ref.html#qapidoc-357
- https://en.wikibooks.org/wiki/QEMU/Monitor#block_resize
- https://qemu-project.gitlab.io/qemu/interop/qemu-qmp-ref.html#qapidoc-421
- https://qemu-project.gitlab.io/qemu/interop/qemu-qmp-ref.html#qapidoc-470

### virsh blockresize

An easier alternative is `virsh blockresize`. We can get the device name with `dumpxml`

```sh
virsh dumpxml <machine>
```

```xml
<domain type='kvm' id='1'>
  <devices>
    <disk type='file' device='disk'>
      <driver name='qemu' type='qcow2'/>
      <source file='/var/lib/libvirt/images/machine.img' index='2'/>
      <target dev='vda' bus='virtio'/>
      <alias name='ua-box-volume-0'/>
    </disk>
  </devices>
</domain>
```

look for `target dev=<target>`. We can also use

```sh
virsh domblklist <machine> --details
```

```sh
 Type   Device   Target   Source
---------------------------------------------------------------------
 file   disk     vda      /var/lib/libvirt/images/machine.img
 file   disk     vdb      /var/lib/libvirt/images/machine-vdb.raw
```

```sh
virsh blockresize <machine> <device> <size>
```

References:

- https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/6/html/virtualization_administration_guide/sub-sect-domain_commands-using_blockresize_to_change_the_size_of_a_domain_path
