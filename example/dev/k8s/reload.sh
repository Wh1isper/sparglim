#!/usr/bin/env bash

kubectl get pod -n sparglim |grep sparglim-app | awk '{print $(1)}' |xargs kubectl delete pod -n sparglim
