### Visibility Receive
Execute a mock “observation” by executing the following script:

```bash
mkdir data
docker run -v `pwd`/data:/data docker.io/pw410/ska-sdp-mock:0.1 /scripts/generate_visibilities.sh /data/out.ms
```

### Visibility Processing
For each observation, you can “process” the data by executing:

```bash
docker run -v `pwd`/data:/data docker.io/pw410/ska-sdp-mock:0.1 /scripts/process_visibilities.sh /data/out.ms /data/out
```