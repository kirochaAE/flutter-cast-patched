import 'dart:typed_data';

/// Incrementally decodes Cast's four-byte big-endian length-prefixed frames.
class CastFrameDecoder {
  static const maxFrameLength = 1024 * 1024;

  final _header = Uint8List(4);
  int _headerLength = 0;
  Uint8List? _body;
  int _bodyLength = 0;

  Iterable<Uint8List> add(List<int> chunk) sync* {
    var offset = 0;
    while (offset < chunk.length) {
      if (_body == null) {
        while (_headerLength < 4 && offset < chunk.length) {
          _header[_headerLength++] = chunk[offset++];
        }
        if (_headerLength < 4) return;
        final length = ByteData.sublistView(_header).getUint32(0);
        if (length == 0 || length > maxFrameLength) {
          throw FormatException('Invalid Cast frame length: $length');
        }
        _body = Uint8List(length);
      }

      final body = _body!;
      final remaining = body.length - _bodyLength;
      final available = chunk.length - offset;
      final count = remaining < available ? remaining : available;
      body.setRange(_bodyLength, _bodyLength + count, chunk, offset);
      _bodyLength += count;
      offset += count;
      if (_bodyLength == body.length) {
        _body = null;
        _bodyLength = 0;
        _headerLength = 0;
        yield body;
      }
    }
  }

  void finish() {
    if (_headerLength != 0 || _body != null) {
      throw const FormatException('Truncated Cast frame');
    }
  }
}
