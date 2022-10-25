#!/bin/bash
# shellcheck shell=bash

set -e

current_version=$(ruby -e "require '$(pwd)/lib/delayed/version.rb'; puts Delayed::VERSION;")
existing_versions=$(gem list --exact inst-jobs --remote --all | grep -o '\((.*)\)$' | tr -d '() ')

if [[ $existing_versions == *$current_version* ]]; then
  echo "Gem has already been published ... skipping ..."
else
  gem build ./inst-jobs.gemspec
  find inst-jobs-*.gem | xargs gem push
fi
