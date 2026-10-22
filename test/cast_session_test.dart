// Explicit endpoint actions document routing and transition ordering.
// ignore_for_file: cascade_invocations

import 'dart:async';
import 'dart:convert';

import 'package:cast/cast.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support/cast_protocol_endpoint.dart';

void main() {
  late CastProtocolEndpoint peer;
  late CastSession session;
  late List<CastSessionState> states;
  late List<Map<String, dynamic>> messages;
  late List<Object> errors;
  late StreamSubscription<CastSessionState> stateSubscription;
  late StreamSubscription<Map<String, dynamic>> messageSubscription;

  setUp(() {
    peer = CastProtocolEndpoint();
    session = CastSession.fromSocket('sender-1', peer.socket);
    states = [];
    messages = [];
    errors = [];
    stateSubscription = session.stateStream.listen(states.add);
    messageSubscription = session.messageStream.listen(
      messages.add,
      onError: (Object error) => errors.add(error),
    );
  });

  tearDown(() async {
    await session.close();
    await peer.dispose();
    await stateSubscription.cancel();
    await messageSubscription.cancel();
  });

  test('empty and malformed application lists never index or cast blindly', () {
    for (final applications in [
      [],
      null,
      42,
      'bad',
      {},
      [
        null,
        4,
        {},
        {'transportId': 4},
      ],
      [
        {'transportId': ''},
      ],
      [
        {'transportId': ' '},
      ],
      [
        {'transportId': 'receiver-0'},
      ],
      [
        {'transportId': '*'},
      ],
    ]) {
      peer.status(applications);
      expect(session.state, CastSessionState.connecting);
    }
    peer.receive(CastSession.kNamespaceReceiver, {
      'type': 'RECEIVER_STATUS',
      'status': 'malformed',
    });
    expect(errors, isEmpty);
    peer.status([null, peer.application('app-1')]);
    expect(session.state, CastSessionState.connected);
    expect(states, [CastSessionState.connected]);
  });

  test(
    'receiver control remains on receiver; media needs an app channel',
    () async {
      expect(
        () =>
            session.sendMessage(CastSession.kNamespaceMedia, {'type': 'LOAD'}),
        throwsStateError,
      );
      session.sendMessage(CastSession.kNamespaceReceiver, {
        'type': 'LAUNCH',
        'appId': 'CC1AD845',
      });
      peer.status([
        peer.application('other', appId: 'OTHER'),
        peer.application('app-1'),
      ]);
      session.sendMessage(CastSession.kNamespaceReceiver, {
        'type': 'GET_STATUS',
      });
      session.sendMessage(CastSession.kNamespaceReceiver, {
        'type': 'SET_VOLUME',
      });
      session.sendMessage(CastSession.kNamespaceMedia, {'type': 'LOAD'});
      await peer.flush();
      expect(peer.sent.map((message) => message.destinationId), [
        'receiver-0',
        'receiver-0',
        'app-1',
        'receiver-0',
        'receiver-0',
        'app-1',
      ]);
    },
  );

  test(
    'replacement invalidates synchronously and opens only the new app',
    () async {
      peer.status([peer.application('app-1')]);
      peer.status([peer.application('app-2')]);
      session.sendMessage(CastSession.kNamespaceMedia, {'type': 'LOAD'});
      await peer.flush();
      expect(states, [
        CastSessionState.connected,
        CastSessionState.connecting,
        CastSessionState.connected,
      ]);
      final connections = peer.sent.where(
        (message) => message.namespace == CastSession.kNamespaceConnection,
      );
      expect(
        connections.map(
          (message) => [
            message.destinationId,
            (jsonDecode(message.payloadUtf8) as Map<String, dynamic>)['type'],
          ],
        ),
        [
          ['receiver-0', 'CONNECT'],
          ['app-1', 'CONNECT'],
          ['app-1', 'CLOSE'],
          ['app-2', 'CONNECT'],
        ],
      );
      expect(peer.sent.last.destinationId, 'app-2');
    },
  );

  test('same transport with a restarted app session resets readiness', () {
    peer.status([peer.application('app-1')]);
    peer.status([peer.application('app-1')]);
    expect(states, [CastSessionState.connected]);
    peer.status([peer.application('app-1', sessionId: 'restart')]);
    expect(states, [
      CastSessionState.connected,
      CastSessionState.connecting,
      CastSessionState.connected,
    ]);
  });

  test('application stop and malformed list gate media until recovery', () {
    peer.status([peer.application('app-1')]);
    peer.status([]);
    expect(session.state, CastSessionState.connecting);
    expect(
      () => session.sendMessage(CastSession.kNamespaceMedia, {'type': 'PAUSE'}),
      throwsStateError,
    );
    peer.status('malformed');
    peer.status([peer.application('app-2')]);
    expect(session.state, CastSessionState.connected);
  });

  test('volume-only and malformed status do not erase a known transport', () {
    peer.status([peer.application('app-1')]);
    for (final status in [
      null,
      [],
      'bad',
      {
        'volume': {'level': 0.4},
      },
    ]) {
      peer.receive(CastSession.kNamespaceReceiver, {
        'type': 'RECEIVER_STATUS',
        'status': status,
      });
    }
    expect(session.state, CastSessionState.connected);
    expect(states, [CastSessionState.connected]);
  });

  test(
    'old or foreign transports and other sender destinations are ignored',
    () {
      peer.status([peer.application('app-1')]);
      peer.status([peer.application('app-2')]);
      messages.clear();
      for (final source in ['app-1', 'foreign', 'receiver-0']) {
        peer.receive(
            CastSession.kNamespaceMedia,
            {
              'type': 'MEDIA_STATUS',
            },
            source: source);
      }
      peer.receive(
        CastSession.kNamespaceMedia,
        {'type': 'MEDIA_STATUS'},
        source: 'app-2',
        destination: 'another-sender',
      );
      peer.receive(
          CastSession.kNamespaceReceiver,
          {
            'type': 'RECEIVER_STATUS',
            'status': {'applications': []},
          },
          source: 'app-2');
      expect(messages, isEmpty);
      expect(session.state, CastSessionState.connected);
      peer.receive(
          CastSession.kNamespaceMedia,
          {
            'type': 'MEDIA_STATUS',
          },
          source: 'app-2');
      expect(messages, hasLength(1));
    },
  );

  test(
    'heartbeats reply to their own channel without surfacing payloads',
    () async {
      peer.status([peer.application('app-1')]);
      messages.clear();
      peer.receive(CastSession.kNamespaceHeartbeat, {'type': 'PING'});
      peer.receive(
          CastSession.kNamespaceHeartbeat,
          {
            'type': 'PING',
          },
          source: 'app-1');
      await peer.flush();
      final pongs = peer.sent.where(
        (message) => message.namespace == CastSession.kNamespaceHeartbeat,
      );
      expect(pongs.map((message) => message.destinationId), [
        'receiver-0',
        'app-1',
      ]);
      expect(messages, isEmpty);
    },
  );

  test(
    'app CLOSE retains receiver; stale CLOSE cannot close replacement',
    () async {
      peer.status([peer.application('app-1')]);
      messages.clear();
      peer.receive(
          CastSession.kNamespaceConnection,
          {
            'type': 'CLOSE',
          },
          source: 'app-1');
      expect(session.state, CastSessionState.connecting);
      peer.status([peer.application('app-2')]);
      peer.receive(
          CastSession.kNamespaceConnection,
          {
            'type': 'CLOSE',
          },
          source: 'app-1');
      expect(session.state, CastSessionState.connected);
      await peer.flush();
      expect(
        peer.sent.any(
          (message) =>
              message.namespace == CastSession.kNamespaceReceiver &&
              message.destinationId == 'receiver-0' &&
              message.payloadUtf8.contains('GET_STATUS'),
        ),
        isTrue,
      );
      expect(messages.where((message) => message['type'] == 'CLOSE'), isEmpty);
    },
  );

  test(
    'receiver CLOSE and repeated local close terminate exactly once',
    () async {
      peer.status([peer.application('app-1')]);
      peer.receive(CastSession.kNamespaceConnection, {'type': 'CLOSE'});
      await session.close();
      await session.close();
      expect(session.state, CastSessionState.closed);
      expect(
        states.where((state) => state == CastSessionState.closed),
        hasLength(1),
      );
      expect(
        () => session.sendMessage(CastSession.kNamespaceReceiver, {}),
        throwsStateError,
      );
    },
  );

  test('truncated endpoint reports error and closes the session', () async {
    peer.input.add([0, 0]);
    await peer.input.close();
    await peer.socket.close();
    expect(errors, [isA<FormatException>()]);
    expect(session.state, CastSessionState.closed);
  });
}
