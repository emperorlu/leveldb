#pragma once

#include <string>

namespace mousika {

  typedef std::string Buf_t;

  class Marshal {
    public:
      static Buf_t get_buffer(int size) { return std::string(size, '0'); }

      static Buf_t forward(const Buf_t& b, int pos, int size) {
        return b.substr(pos + size, b.size() - size);
      }

      template <typename T>
      static void* serialize_to_buf(const T& t, const void* buf) {
        memcpy((void*)buf, &t, sizeof(T));
        return static_cast<void*>((char*)buf + sizeof(T));
      }

      template <typename T>
      static Buf_t serialize_to_buf(const T& t) {
        auto res = get_buffer(sizeof(T));
        serialize_to_buf<T>(t, res.data());
        return res;
      }

      template <typename T>
      static void serialize_append(Buf_t& b, const T& t) {
        auto append = serialize_to_buf(t);
        b.append(append);
      }

      template <typename T>
      static bool deserialize(const Buf_t& buf, T& t) {
        if (buf.size() < sizeof(T)) return false;
        memcpy((char*)(&t), buf.data(), sizeof(T));
        return true;
      }

      template <typename T>
      static T deserialize(const Buf_t& buf) {
        T res;
        memcpy((char*)(&res), (char*)buf.data(), std::min(buf.size(), sizeof(T)));
        return res;
      }

      template <typename T>
      static void* deserialize(const void* buf, T& t) {
        memcpy((char*)(&t), buf, sizeof(T));
        return static_cast<void*>((char*)buf + sizeof(T));
      }

      template <typename T>
      static T deserialize(const void* buf) {
        T ret;
        deserialize<T>(buf, ret);
        return ret;
      }
  };

}  // end namespace mousika
