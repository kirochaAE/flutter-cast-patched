import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

import 'package:cast/cast.dart';
import 'package:cast/cast_channel/cast_channel.pb.dart';
import 'package:cast/src/frame_decoder.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support/cast_protocol_endpoint.dart';

void main() {
  late CastProtocolEndpoint peer;
  late List<CastSocketMessage> messages;
  late List<Object> errors;
  late StreamSubscription<CastSocketMessage> subscription;

  setUp(() {
    peer = CastProtocolEndpoint();
    messages = [];
    errors = [];
    subscription = peer.socket.stream.listen(
      messages.add,
      onError: (Object error) => errors.add(error),
    );
  });

  tearDown(() async {
    await peer.dispose();
    await subscription.cancel();
  });

  test('every byte boundary can split a header or protobuf body', () {
    final frame = CastProtocolEndpoint.frame('test', {'type': 'PING'});
    for (var split = 1; split < frame.length; split++) {
      peer.input.add(frame.sublist(0, split));
      expect(messages, hasLength(split - 1));
      peer.input.add(frame.sublist(split));
      expect(messages, hasLength(split));
    }
    expect(errors, isEmpty);
    expect(messages.last.sourceId, 'receiver-0');
    expect(messages.last.destinationId, 'sender-1');
    expect(messages.last.payload, {'type': 'PING'});
  });

  test('one-byte chunks and coalesced frames retain message ordering', () {
    final first = CastProtocolEndpoint.frame('test', {'n': 1});
    final second = CastProtocolEndpoint.frame('test', {'n': 2});
    for (final byte in first) {
      peer.input.add([byte]);
    }
    peer.input.add([...first, ...second, ...second.sublist(0, 3)]);
    expect(messages.map((message) => message.payload['n']), [1, 1, 2]);
    peer.input.add(second.sublist(3));
    expect(messages.map((message) => message.payload['n']), [1, 1, 2, 2]);
    expect(errors, isEmpty);
  });

  for (final length in [0, CastFrameDecoder.maxFrameLength + 1, 0xffffffff]) {
    test('invalid length $length fails once and closes safely', () async {
      final header = Uint8List(4);
      ByteData.sublistView(header).setUint32(0, length);
      final done = Completer<void>();
      final completion = peer.socket.stream.listen(
        (_) {},
        onError: (Object error) {},
        onDone: done.complete,
      );
      peer.input.add(header);
      await done.future.timeout(const Duration(seconds: 1));
      await completion.cancel();
      await peer.socket.close();
      expect(errors, [isA<FormatException>()]);
      expect(messages, isEmpty);
      expect(
        () => peer.socket.sendMessage('test', 'a', 'b', {}),
        throwsStateError,
      );
    });
  }

  for (final count in [1, 3, 4, 8]) {
    test('EOF after $count bytes reports a truncated frame', () async {
      final frame = CastProtocolEndpoint.frame('test', {'n': 1});
      peer.input.add(frame.sublist(0, count));
      await peer.input.close();
      await peer.socket.close();
      expect(errors, [isA<FormatException>()]);
      expect(messages, isEmpty);
    });
  }

  test('clean EOF closes without a truncation error', () async {
    peer.receive('test', {'n': 1});
    await peer.input.close();
    await peer.socket.close();
    expect(messages, hasLength(1));
    expect(errors, isEmpty);
  });

  test('malformed protobuf fails without an uncaught parsing error', () async {
    peer.input.add(CastProtocolEndpoint.frameBytes([0xff]));
    await peer.socket.close();
    expect(errors, hasLength(1));
    expect(messages, isEmpty);
  });

  test('incomplete required protobuf envelope is rejected', () async {
    peer.input.add(CastProtocolEndpoint.frameBytes([0x32, 0x02, 0x7b, 0x7d]));
    await peer.socket.close();
    expect(errors, hasLength(1));
  });

  test('invalid JSON is reported and closes the endpoint', () async {
    final message = CastMessage(
      protocolVersion: CastMessage_ProtocolVersion.CASTV2_1_0,
      sourceId: 'receiver-0',
      destinationId: 'sender-1',
      namespace: 'test',
      payloadType: CastMessage_PayloadType.STRING,
      payloadUtf8: '{',
    );
    peer.input.add(CastProtocolEndpoint.frameBytes(message.writeToBuffer()));
    await peer.socket.close();
    expect(errors, [isA<FormatException>()]);
  });

  test('a paused subscriber cannot block socket teardown', () async {
    subscription.pause();
    peer.receive('test', {'queued': true});
    await peer.socket.close().timeout(const Duration(seconds: 1));
    subscription.resume();
  });

  for (final payload in [
    'not an object',
    [1, 2],
    null,
  ]) {
    test('non-object JSON $payload is rejected', () async {
      peer.receive('test', payload);
      await peer.socket.close();
      expect(errors, [isA<FormatException>()]);
    });
  }

  test('binary auth is not decoded as JSON; the next frame survives', () {
    final binary = CastMessage(
      protocolVersion: CastMessage_ProtocolVersion.CASTV2_1_0,
      sourceId: 'receiver-0',
      destinationId: 'sender-1',
      namespace: CastSession.kNamespaceDeviceauth,
      payloadType: CastMessage_PayloadType.BINARY,
      payloadBinary: [1, 2, 3],
    );
    peer.input.add([
      ...CastProtocolEndpoint.frameBytes(binary.writeToBuffer()),
      ...CastProtocolEndpoint.frame('test', {'ok': true}),
    ]);
    expect(messages.single.payload, {'ok': true});
    expect(errors, isEmpty);
  });

  test('stream error terminates and teardown remains idempotent', () async {
    peer.input.addError(StateError('injected read failure'));
    await peer.socket.close();
    expect(errors, [isA<StateError>()]);
  });

  test('output sink failure automatically reports and closes', () async {
    final input = StreamController<List<int>>();
    final output = IOSink(_FailingOutput());
    final socket = CastSocket.fromStreams(input.stream, output);
    final failures = <Object>[];
    final done = Completer<void>();
    final listener = socket.stream.listen(
      (_) {},
      onError: failures.add,
      onDone: done.complete,
    );
    try {
      socket.sendMessage('test', 'sender', 'receiver', {});
      await done.future.timeout(const Duration(seconds: 1));
      expect(failures, [isA<StateError>()]);
      expect(() => socket.sendMessage('test', 'a', 'b', {}), throwsStateError);
    } finally {
      await socket.close();
      await input.close();
      await output.close().onError((error, stackTrace) {});
      await listener.cancel();
    }
  });

  test(
    'outgoing frames use big-endian lengths without mutating input',
    () async {
      final payload = <String, dynamic>{'type': 'GET_STATUS'};
      peer.socket.sendMessage('test', 'a', 'b', payload);
      peer.socket.sendMessage('test', 'a', 'c', {'requestId': 99});
      await peer.flush();
      expect(payload, {'type': 'GET_STATUS'});
      expect(peer.sent.map((message) => message.destinationId), ['b', 'c']);
      expect(peer.sent.first.payloadUtf8, contains('"requestId":0'));
      expect(peer.sent.last.payloadUtf8, contains('"requestId":99'));
    },
  );
}

class _FailingOutput implements StreamConsumer<List<int>> {
  @override
  Future<void> addStream(Stream<List<int>> stream) async {
    await for (final _ in stream) {
      throw StateError('Injected write failure');
    }
  }

  @override
  Future<void> close() async {}
}
