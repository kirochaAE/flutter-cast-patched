import 'dart:async';

import 'device.dart';
import 'socket.dart';

enum CastSessionState { connecting, connected, closed }

class CastSession {
  /// Attaches to an existing protocol endpoint without discovery or TLS setup.
  CastSession.fromSocket(
    this.sessionId,
    this._socket, {
    this.applicationRecoveryTimeout = const Duration(seconds: 15),
  }) {
    _subscription = _socket.stream.listen(
      _handleMessage,
      onError: (Object error, StackTrace stackTrace) {
        if (!_closing) {
          _messageController.addError(error, stackTrace);
          _envelopeController.addError(error, stackTrace);
        }
      },
      onDone: _finish,
    );
    _sendTo(kNamespaceConnection, 'receiver-0', {'type': 'CONNECT'});
  }

  static const kNamespaceConnection =
      'urn:x-cast:com.google.cast.tp.connection';
  static const kNamespaceHeartbeat = 'urn:x-cast:com.google.cast.tp.heartbeat';
  static const kNamespaceReceiver = 'urn:x-cast:com.google.cast.receiver';
  static const kNamespaceDeviceauth =
      'urn:x-cast:com.google.cast.tp.deviceauth';
  static const kNamespaceMedia = 'urn:x-cast:com.google.cast.media';

  final String sessionId;
  final CastSocket _socket;
  final Duration applicationRecoveryTimeout;
  CastSocket get socket => _socket;
  CastSessionState get state => _state;
  Stream<CastSessionState> get stateStream => _stateController.stream;
  Stream<Map<String, dynamic>> get messageStream => _messageController.stream;
  Stream<CastSocketMessage> get envelopeStream => _envelopeController.stream;

  CastSessionState _state = CastSessionState.connecting;
  String? _transportId;
  String? _applicationSessionId;
  String? _targetAppId;
  Timer? _recoveryTimer;
  bool _closing = false;
  Future<void>? _closeFuture;
  late final StreamSubscription<CastSocketMessage> _subscription;
  // Deliver invalidation before any message from a replacement transport.
  final _stateController =
      StreamController<CastSessionState>.broadcast(sync: true);
  final _messageController =
      StreamController<Map<String, dynamic>>.broadcast(sync: true);
  final _envelopeController =
      StreamController<CastSocketMessage>.broadcast(sync: true);

  static Future<CastSession> connect(
    String sessionId,
    CastDevice device, [
    Duration? timeout,
  ]) async {
    final socket = await CastSocket.connect(device.host, device.port, timeout);
    return CastSession.fromSocket(sessionId, socket);
  }

  void _handleMessage(CastSocketMessage message) {
    if (_closing ||
        (message.destinationId != sessionId && message.destinationId != '*')) {
      return;
    }
    final fromReceiver = message.sourceId == 'receiver-0';
    final fromApplication = message.sourceId == _transportId;
    if (!fromReceiver && !fromApplication) return;

    if (message.namespace == kNamespaceHeartbeat) {
      if (message.payload['type'] == 'PING') {
        _sendTo(kNamespaceHeartbeat, message.sourceId, {'type': 'PONG'});
      }
      return;
    }
    if (message.namespace == kNamespaceConnection) {
      if (message.payload['type'] == 'CLOSE') {
        if (fromReceiver) {
          unawaited(close());
        } else {
          _invalidateTransport();
          _sendTo(kNamespaceReceiver, 'receiver-0', {'type': 'GET_STATUS'});
        }
      }
      return;
    }
    if (message.namespace == kNamespaceReceiver) {
      if (!fromReceiver) return;
      if (message.payload['type'] == 'RECEIVER_STATUS') {
        _handleReceiverStatus(message.payload);
      }
    } else if (!fromApplication) {
      return;
    }
    if (!_closing) {
      _messageController.add(message.payload);
      _envelopeController.add(message);
    }
  }

  void _handleReceiverStatus(Map<String, dynamic> payload) {
    final status = payload['status'];
    if (status is! Map || !status.containsKey('applications')) return;
    final applications = status['applications'];
    Map? application;
    if (applications is List) {
      for (final candidate in applications) {
        if (candidate is! Map) continue;
        final transport = candidate['transportId'];
        if (transport is! String ||
            transport.trim().isEmpty ||
            transport == 'receiver-0' ||
            transport == sessionId ||
            transport == '*') continue;
        if (_targetAppId != null && candidate['appId'] != _targetAppId)
          continue;
        application = candidate;
        break;
      }
    }
    if (application == null) {
      _invalidateTransport();
      return;
    }
    final transportId = application['transportId'] as String;
    final appSession = application['sessionId'];
    final applicationSessionId = appSession is String ? appSession : null;
    if (_transportId == transportId &&
        _applicationSessionId == applicationSessionId) return;

    _invalidateTransport();
    _sendTo(kNamespaceConnection, transportId, {'type': 'CONNECT'});
    _transportId = transportId;
    _applicationSessionId = applicationSessionId;
    _recoveryTimer?.cancel();
    _recoveryTimer = null;
    _setState(CastSessionState.connected);
  }

  void _invalidateTransport() {
    final previous = _transportId;
    _transportId = null;
    _applicationSessionId = null;
    _setState(CastSessionState.connecting);
    if (previous != null) {
      _recoveryTimer ??= Timer(applicationRecoveryTimeout, () {
        unawaited(close());
      });
      _sendTo(kNamespaceConnection, previous, {'type': 'CLOSE'});
    }
  }

  void _setState(CastSessionState state) {
    if (_state == state) return;
    _state = state;
    _stateController.add(state);
  }

  void sendMessage(String namespace, Map<String, dynamic> payload) {
    if (_closing) throw StateError('Cast session is closed');
    if (namespace == kNamespaceReceiver) {
      if (payload['type'] == 'LAUNCH' && payload['appId'] is String) {
        final appId = payload['appId'] as String;
        if (_targetAppId != appId) {
          _targetAppId = appId;
          _invalidateTransport();
        }
      }
      _sendTo(namespace, 'receiver-0', payload);
      return;
    }
    if (namespace == kNamespaceConnection ||
        namespace == kNamespaceHeartbeat ||
        namespace == kNamespaceDeviceauth) {
      _sendTo(namespace, 'receiver-0', payload);
      return;
    }
    final transportId = _transportId;
    if (_state != CastSessionState.connected || transportId == null) {
      throw StateError('Cast application transport is not ready');
    }
    _sendTo(namespace, transportId, payload);
  }

  void _sendTo(
      String namespace, String destination, Map<String, dynamic> payload) {
    _socket.sendMessage(namespace, sessionId, destination, payload);
  }

  Future<void> close() => _closeFuture ??= _close();

  Future<void> _close() async {
    _recoveryTimer?.cancel();
    if (!_closing) {
      _closing = true;
      try {
        if (_transportId != null) {
          _sendTo(kNamespaceConnection, _transportId!, {'type': 'CLOSE'});
        }
        _sendTo(kNamespaceConnection, 'receiver-0', {'type': 'CLOSE'});
        await _socket.flush().timeout(const Duration(seconds: 1));
      } catch (_) {
        // Closing an already-failed endpoint must still release listeners.
      }
    }
    await _socket.close();
    _finish();
    await _subscription.cancel();
  }

  void _finish() {
    if (_state == CastSessionState.closed) return;
    _closing = true;
    _recoveryTimer?.cancel();
    _transportId = null;
    _applicationSessionId = null;
    _setState(CastSessionState.closed);
    unawaited(_messageController.close());
    unawaited(_envelopeController.close());
    unawaited(_stateController.close());
  }

  Future<void> flush() => _socket.flush();
}
