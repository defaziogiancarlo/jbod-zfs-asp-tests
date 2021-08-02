#!/bin/bash

# commands that Camerosn uses for his testing
./ior -C -Q 1 -g -G 271 -k -e -o /p/lflood/harr1/io500-all/$(date +"%Y.%m.%d-%H.%M.%S")/ior-easy/ior_file_easy -O stoneWallingStatusFile=./results/$(date +"%Y.%m.%d-%H.%M.%S")/ior-easy.stonewall -t 2m -b 9920000m -F -w -D 300 -O stoneWallingWearOut=1 -a POSIX
./ior -C -Q 1 -g -G 271 -k -e -o /p/lflood/harr1/io500-all/$(date +"%Y.%m.%d-%H.%M.%S")/ior-easy/ior_file_easy -O stoneWallingStatusFile=./results/$(date +"%Y.%m.%d-%H.%M.%S")/ior-easy.stonewall -t 2m -b 9920000m -F -r -R -a POSIX
./mdtest '-n' '1000000' '-u' '-L' '-F' '-P' '-N' '1' '-d' '/p/lflood/harr1/io500-all/$(date +"%Y.%m.%d-%H.%M.%S")/mdtest-easy' '-x' './results/$(date +"%Y.%m.%d-%H.%M.%S")/mdtest-easy.stonewall' '-C' '-Y' '-W' '300' '-a' 'POSIX'
./mdtest '-n' '1000000' '-u' '-L' '-F' '-P' '-N' '1' '-d' '/p/lflood/harr1/io500-all/$(date +"%Y.%m.%d-%H.%M.%S")/mdtest-easy' '-x' './results/$(date +"%Y.%m.%d-%H.%M.%S")/mdtest-easy.stonewall' '-T' '-a' 'POSIX'

# modified for my use
./ior -C -Q 1 -g -G 271 -k -e -o /p/lflood/harr1/io500-all/$(date +"%Y.%m.%d-%H.%M.%S")/ior-easy/ior_file_easy -O stoneWallingStatusFile=./results/$(date +"%Y.%m.%d-%H.%M.%S")/ior-easy.stonewall -t 2m -b 9920000m -F -w -D 300 -O stoneWallingWearOut=1 -a POSIX

./ior -C -Q 1 -g -G 271 -k -e -o /p/lflood/harr1/io500-all/$(date +"%Y.%m.%d-%H.%M.%S")/ior-easy/ior_file_easy -O stoneWallingStatusFile=./results/$(date +"%Y.%m.%d-%H.%M.%S")/ior-easy.stonewall -t 2m -b 9920000m -F -r -R -a POSIX

./mdtest '-n' '1000000' '-u' '-L' '-F' '-P' '-N' '1' '-d' '/p/lflood/harr1/io500-all/$(date +"%Y.%m.%d-%H.%M.%S")/mdtest-easy' '-x' './results/$(date +"%Y.%m.%d-%H.%M.%S")/mdtest-easy.stonewall' '-C' '-Y' '-W' '300' '-a' 'POSIX'

./mdtest '-n' '1000000' '-u' '-L' '-F' '-P' '-N' '1' '-d' '/p/lflood/harr1/io500-all/$(date +"%Y.%m.%d-%H.%M.%S")/mdtest-easy' '-x' './results/$(date +"%Y.%m.%d-%H.%M.%S")/mdtest-easy.stonewall' '-T' '-a' 'POSIX'
