# PowerView to MQTT bridge: Home Assistant Add-On

This addon provides a bridge between a [Hunter Douglas PowerView
Hub](https://www.hunterdouglas.com/operating-systems/motorized/powerview) and
Home Assistant, via the [Home Assistant MQTT
Integration](https://www.home-assistant.io/integrations/mqtt/).

Once you have configured your MQTT service credentials, this addon should
automatically discover your PowerView Hub and import its shades and scenes
via the MQTT integration.

## Building from a checkout

Build from the repository root so the binary and launcher come from the same
revision (including local changes):

```sh
docker buildx build --platform linux/amd64 -f addon/Dockerfile .
```

For ARM, also pass the matching `BUILD_FROM` image from `addon/build.yaml`.
Release CI checks out the release tag and uses this same repository context;
PR builds compile the checked-out PR source.
