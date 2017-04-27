# Tackling Misinformation with Big Data
## A project by Team Swashbucklers for ATLS 5214 @ CU Boulder
----
#### Introduction

This project aims to highlight the spread of misinformation through social media by using Map and graph-based visualizations 
to gain insight into how misinformation spreads across the internet. We shall be using Twitter to conduct our experiments.

#### How it works

Our aim is to track the spread of false news and hoaxes by using fact-checking websites such as Snopes, PolitiFact and 
FactCheck.

#### Roadmap
- [x] Get Factchecking Links from Websites(Snopes for now)
- [x] Get list of corresponding tweets from Twitter(Using GNIP).
- [ ] Map visualization to show the distributions of the hoax
- [ ] Map visualization to show timeline of the spread of the hoax
- [ ] Graph visualization to show how Hoax spreads across news. 

#### Deployment Network

The current network is deployed as follows:

```
i-0bc4e30e7f18e4426: ec2-35-163-127-184.us-west-2.compute.amazonaws.com
i-0cfbad38c334376b5: ec2-52-40-190-188.us-west-2.compute.amazonaws.com
i-0552ae0ed53824adc: ec2-35-161-237-71.us-west-2.compute.amazonaws.com
i-07c9c5f5051bbbdae: ec2-35-167-172-19.us-west-2.compute.amazonaws.com
i-0836f0686ee0c7c54: ec2-34-209-252-154.us-west-2.compute.amazonaws.com
i-08fdfc67bc499eab8: ec2-52-24-56-102.us-west-2.compute.amazonaws.com
i-0a81edc8a5a617ff2: ec2-34-208-42-57.us-west-2.compute.amazonaws.com
i-0be5863aa52425b99: ec2-54-148-40-244.us-west-2.compute.amazonaws.com
i-0d3ae1fa9bf9057f8: ec2-35-161-215-169.us-west-2.compute.amazonaws.com
i-0ddbab677cd26d922: ec2-35-166-110-70.us-west-2.compute.amazonaws.com
```

The instance `i-0bc4e30e7f18e4426` is a `t2.large` with two 100GB disks
attached (`/dev/sda1` and `/dev/sdf`). The root is located on `/dev/sda1`,
and the other device is mounted but unused.

All other instances are `t2.medium`. The Kubernetes master lives on
`i-0bc4e30e7f18e4426`, which contains an 8GB root disk. The other instances
are joined in the cluster with 125GB root disks.

All machines, excluding the development machine, sit behind an ELB balancer
located at `sb-k8s-ingress-1525602578.us-west-2.elb.amazonaws.com`, which
is a dual-redundant DNS A-record. The following ingress mappings are set:

- 4040:31821
- 6066:31788
- 7077:32219
- 8080:32505
- 9042:30171

These correspond to cluster-wide port mappings for running services on
Kubernetes. All components reside in the `vpc-17145c70` network in the
`us-west-2a` region.
