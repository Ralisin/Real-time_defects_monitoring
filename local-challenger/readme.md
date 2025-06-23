## Setup

To run local challenger `/data` folder must be downloaded from `Database with OT images` and placed in this folder root. Same must be done with `gc25cdocker.tar` file, that can be downloaded from `All files` link.

- [README](http://www.ce.uniroma2.it/courses/sabd2425/project/README.md)
- [All files, except for database](http://www.ce.uniroma2.it/courses/sabd2425/project/gc25-chall.tgz)
- [Database with OT images](http://www.ce.uniroma2.it/courses/sabd2425/project/gc25-chall-data.tgz)

## Execution

After setup, to load docker image and start server, the following commands must be written in the terminal. To simplify the process there is `run.sh` script file.

```bash
  docker image load -i gc25cdocker.tar
```

```bash
  docker run -p 8866:8866 -v ./data:/data micro-challenger:latest 0.0.0.0:8866 /data
```

## Server info

Inside there are _3600_ batches