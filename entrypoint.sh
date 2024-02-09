#!/bin/bash

IFS=' ' read -r -a args_array <<<"$1"
args=()
for arg in "${args_array[@]}"; do
    echo "Adding argument: $arg"
    args+=("$arg")
done

pre-commit "${args[@]}"
