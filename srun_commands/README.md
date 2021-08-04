These are commands that were run (or attempted to run)
at the time of the filename. Really, the timestamp is created,
then file is written, then an srun command command is run
using the file, so the srun command happens a little after the file
is written, and of course then the command gets scheduled and might
not run for a while.

TODO: include the full srun command in commented form in the file.
This may not be neccesary, `mdtest` includes the command and the number
of nodes and processes per node in the test results, need to check
what `ior` does.
