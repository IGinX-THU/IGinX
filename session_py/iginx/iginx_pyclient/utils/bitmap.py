class Bitmap(object):
    BIT_UTIL = [1, 1 << 1, 1 << 2, 1 << 3, 1 << 4, 1 << 5, 1 << 6, 1 << 7]

    def __init__(self, size, bits=None):
        self.__size = size
        if bits is None:
            self.bits = []
            for i in range(size // 8 + 1):
                self.bits.append(0)
        else:
            self.bits = bits


    def set(self, position):
        self.bits[position // 8] |= Bitmap.BIT_UTIL[position % 8]


    def get(self, position):
        return (self.bits[position // 8] & Bitmap.BIT_UTIL[position % 8]) == Bitmap.BIT_UTIL[position % 8]


    def get_bytes(self):
        return self.bits