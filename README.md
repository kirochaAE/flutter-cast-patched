# cast

Dart package to discover and connect with Chromecast devices

## Installation

This is the patched fork of [johnvuko/flutter_cast](https://github.com/johnvuko/flutter_cast)
2.1.0 used by ScreenLinker. The package name and public API remain `cast`.
The upstream MIT license and copyright are preserved in `LICENSE`.

Use this Git dependency, not the unpatched hosted `cast: ^2.1.0` release:

```yaml
dependencies:
  cast:
    git:
      url: https://github.com/kirochaAE/flutter-cast-patched.git
      ref: main
```

For reproducible builds, replace `main` with the reviewed full commit SHA.

The fork fixes receiver/application channel routing and application lifecycle
recovery, validates receiver status, and handles fragmented/coalesced TCP frames,
malformed messages, and socket teardown. Runtime source and generated protobufs
are preserved from ScreenLinker's patched package.

Runtime dependencies are pinned to the supported Bonsoir `5.1.11` and protobuf
`3.1.0`. Upstream's open-ended Bonsoir constraint selected `7.1.5` on a fresh
resolution, whose discovery API is incompatible with this source. This fork uses
exact dependency versions rather than that incompatible broad range.

Run `flutter analyze` and `flutter test` from this package root. The standalone
session/socket regressions use in-memory protocol endpoints only; they do not
verify real-device discovery, TLS connectivity, or Chromecast playback.

### iOS

Required iOS 13 minimum.
Since iOS 14 a few more steps are required https://developers.google.com/cast/docs/ios_sender/ios_permissions_changes

In `ios/Runner/Info.plist` add:

```xml
<key>NSBonjourServices</key>
<array>
  <string>_googlecast._tcp</string>
  <string>_YOUR_APP_ID._googlecast._tcp</string>
</array>

<key>NSLocalNetworkUsageDescription</key>
<string>${PRODUCT_NAME} uses the local network to discover Cast-enabled devices on your WiFi
network.</string>
```

## Usage

List devices:

```
Widget build(BuildContext context) {
  return FutureBuilder<List<CastDevice>>(
    future: CastDiscoveryService().search(),
    builder: (context, snapshot) {
      if (snapshot.hasError) {
        return Center(
          child: Text(
            'Error: ${snapshot.error.toString()}',
          ),
        );
      } else if (!snapshot.hasData) {
        return Center(
          child: CircularProgressIndicator(),
        );
      }

      if (snapshot.data!.isEmpty) {
        return Column(
          children: [
            Center(
              child: Text(
                'No Chromecast founded',
              ),
            ),
          ],
        );
      }

      return Column(
        children: snapshot.data!.map((device) {
          return ListTile(
            title: Text(device.name),
            onTap: () {
              _connectToYourApp(context, device);
            },
          );
        }).toList(),
      );
    },
  );
}
```

Connect to device:

```
Future<void> _connect(BuildContext context, CastDevice object) async {
  final session = await CastSessionManager().startSession(object);

  session.stateStream.listen((state) {
    if (state == CastSessionState.connected) {
      _sendMessage(session);
    }
  });

  session.messageStream.listen((message) {
    print('receive message: $message');
  });

  session.sendMessage(CastSession.kNamespaceReceiver, {
    'type': 'LAUNCH',
    'appId': 'YouTube', // set the appId of your app here
  });
}
```

`CastSessionManager` is used to keep track of all sessions.

Send message:

```
void _sendMessage(CastSession session) {
  session.sendMessage('urn:x-cast:namespace-of-the-app', {
    'type': 'sample',
  });
}
```

Except for the launch message, you should wait until the session have a connected state before sending message.

## Note

Some informations about the protocol used https://docs.rs/crate/gcast/0.1.5/source/PROTOCOL.md

    $ pub global activate protoc_plugin
    $ export PATH="$PATH":"$HOME/.pub-cache/bin"
    $ protoc -I=./lib/cast_channel --dart_out=./lib/cast_channel ./lib/cast_channel/cast_channel.proto --plugin "pub run protoc_plugin"

## Author

- [Jonathan VUKOVICH](https://github.com/johnvuko)
