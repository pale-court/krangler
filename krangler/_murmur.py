def bytes_to_long(bytes):
    assert len(bytes) == 8
    return sum((b << (k * 8) for k, b in enumerate(bytes)))

# https://gist.github.com/wey-gu/5543c33987c0a5e8f7474b9b80cd36aa
def murmur2_64a(data, seed=0x1337B33F):
    import ctypes

    m = ctypes.c_uint64(0xC6A4A7935BD1E995).value

    r = ctypes.c_uint32(47).value

    MASK = ctypes.c_uint64(2**64 - 1).value

    data_as_bytes = bytearray(data)

    seed = ctypes.c_uint64(seed).value

    h = seed ^ ((m * len(data_as_bytes)) & MASK)

    off = int(len(data_as_bytes) / 8) * 8
    for ll in range(0, off, 8):
        k = bytes_to_long(data_as_bytes[ll : ll + 8])
        k = (k * m) & MASK
        k = k ^ ((k >> r) & MASK)
        k = (k * m) & MASK
        h = h ^ k
        h = (h * m) & MASK

    length = len(data_as_bytes) & 7

    if length >= 7:
        h = h ^ (data_as_bytes[off + 6] << 48)

    if length >= 6:
        h = h ^ (data_as_bytes[off + 5] << 40)

    if length >= 5:
        h = h ^ (data_as_bytes[off + 4] << 32)

    if length >= 4:
        h = h ^ (data_as_bytes[off + 3] << 24)

    if length >= 3:
        h = h ^ (data_as_bytes[off + 2] << 16)

    if length >= 2:
        h = h ^ (data_as_bytes[off + 1] << 8)

    if length >= 1:
        h = h ^ data_as_bytes[off]
        h = (h * m) & MASK

    h = h ^ ((h >> r) & MASK)
    h = (h * m) & MASK
    h = h ^ ((h >> r) & MASK)

    return ctypes.c_uint64(h).value
