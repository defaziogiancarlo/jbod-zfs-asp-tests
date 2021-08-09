## Performance testing for garter
We need do some performance testing on `garter` that
looks at `jbod` and `zfs` parameters.
The basic setup is to set the parameters for `garter`,
then run performance tests on `catalyst` and record all the
test parameters and performace data, as well as the `zfs` and `jbod`
parameters.

## Subtasks

### set and check zfs parameters
You need to set values for
- zfs\_dirty\_data\_max (default 10%)
- zfs\_dirty\_data\_max\_max (default 25%)
- zfs\_dirty\_data\_max\_percent (default 10%)
- zfs\_max\_recordsize (default 1048576)

This should give you all the data you need to check zfs values
from garteri
```bash
pdsh -w egarter[1-8] "echo -n 'zfs_dirty_data_max ' && cat /sys/module/zfs/parameters/zfs_dirty_data_max"
pdsh -w egarter[1-8] "echo -n 'zfs_dirty_data_max_percent ' && cat /sys/module/zfs/parameters/zfs_dirty_data_max_percent"
pdsh -w egarter[1-8] "echo -n 'zfs_max_recordsize ' && cat /sys/module/zfs/parameters/zfs_max_recordsize"
pdsh -w egarter[1-8] zfs get -H recordsize
```
I don't see `/etc/modprobe.d/zfs.conf` on garter, is it just using the defaults?

To find the defaults
```bash
man zfs-module-parameters
```


### set and check jbod parameters

There's a confluence page for this
it https://lc.llnl.gov/confluence/pages/viewpage.action?pageId=672764883

The example uses the BMC (or the BMC of the node and not the jbod, or something like that)
You want to contact the `jbod`
You can get the names of the jbods with
```bash
cat etc/hosts | grep jbod
```

Then from the mgmt node, following the example
~defazio1/supermicro\_system\_utils zone -p ADMIN -u ADMIN -h jbod5a -a get

It turns out you might need to physically restart the jbods for this to work.
Just setting stuff via software didn't actually cause the change you wanted.

### run tests on catalyst

look at jira/TOSS/5213/garter_mdtest/ for how you did this for previous tests
How about you create 2 scripts, one for garteri, and one for catalyst
