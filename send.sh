#!/bin/bash

rsync -avzh --info=progress2 -e "ssh -i ~/.ssh/hadoop-master" $1 ypjun100@10.178.0.8:/datasets/
rsync -avzh --info=progress2 -e "ssh -i ~/.ssh/hadoop-master" $1 ypjun100@10.178.0.9:/datasets/