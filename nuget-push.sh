#!/bin/sh

# pushes all generated .nupkg files to nuget

root=`dirname $0`
location="$root/bin"
pkgs=`ls "$location" | grep '.nupkg$'`

if [ -z "$pkgs" ]; then
    echo "no nuget packages found."
    exit 0
fi

echo "Found the following packages:"
for pkg in $pkgs; do
    echo "$pkg"
done

push() {
    for pkg in $pkgs; do
        nuget push "$location/$pkg"
    done

}

read -r -p "Push? [y/N] " response
case $response in
    [yY]) 
        push
        ;;
    *)
        exit 0
        ;;
esac
