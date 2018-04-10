/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <errno.h>
#include <netdb.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <time.h>

#include <thrift/c_glib/thrift.h>
#include <thrift/c_glib/transport/thrift_transport.h>
#include <thrift/c_glib/transport/thrift_udp_socket.h>
#include <thrift/c_glib/protocol/thrift_protocol.h>
#include <thrift/c_glib/protocol/thrift_binary_protocol.h>

/* object properties */
enum _ThriftUDPSocketProperties
{
  PROP_0,
  PROP_THRIFT_UDP_SOCKET_HOSTNAME,
  PROP_THRIFT_UDP_SOCKET_PORT, 
  PROP_THRIFT_UDP_SOCKET_SERVER,
  PROP_THRIFT_UDP_SOCKET_LISTEN_PORT
};

/* for errors coming from socket() and connect() */
extern int errno;

G_DEFINE_TYPE(ThriftUDPSocket, thrift_udp_socket, THRIFT_TYPE_TRANSPORT)

/* implements thrift_transport_is_open */
gboolean
thrift_udp_socket_is_open (ThriftTransport *transport)
{
  ThriftUDPSocket *socket = THRIFT_UDP_SOCKET (transport);
  return socket->sd != THRIFT_INVALID_SOCKET;
}

/* overrides thrift_transport_peek */
gboolean
thrift_udp_socket_peek (ThriftTransport *transport, GError **error)
{
  gboolean result = FALSE;
  guint8 buf;
  int r;
  int errno_copy;
#if defined(USE_IPV6)
  struct sockaddr_in6 src_addr;
#else
  struct sockaddr_in src_addr;
#endif
  socklen_t srcaddrlen = sizeof(src_addr);

  ThriftUDPSocket *socket = THRIFT_UDP_SOCKET (transport);

  // g_message("Checking if socket open\n");
  if (thrift_udp_socket_is_open (transport))
  {
    // g_message("Attempting to recvfrom with peek\n");
    // Attempt to recvfrom the socket (this will block until data exists)
    r = recvfrom (socket->sd, &buf, 1, MSG_PEEK, (struct sockaddr *) &src_addr, &srcaddrlen);
    if (r == -1)
    {
      errno_copy = errno;

      #if defined __FreeBSD__ || defined __MACH__
      /* FreeBSD returns -1 and ECONNRESET if the socket was closed by the other
         side */
      if (errno_copy == ECONNRESET)
      {
        thrift_udp_socket_close (transport, error);
      }
      else
      {
      #endif

      g_set_error (error,
                   THRIFT_TRANSPORT_ERROR,
                   THRIFT_TRANSPORT_ERROR_SOCKET,
                   "failed to peek at socket - %s",
                   strerror (errno_copy));

      #if defined __FreeBSD__ || defined __MACH__
      }
      #endif
    }
    else if (r > 0)
    {
      result = TRUE;
    }
  }

  return result;
}

/* implements thrift_transport_open */
gboolean
thrift_udp_socket_open (ThriftTransport *transport, GError **error)
{
  int enabled = 1;
#if defined(USE_IPV6)
  struct sockaddr_in6 pin;
  struct sockaddr_in6 serveraddr;
  sa_family_t fam = AF_INET6;
#else
  struct sockaddr_in pin;
  struct sockaddr_in serveraddr;
  sa_family_t fam = AF_INET;
#endif
  struct sockaddr_storage result;
  struct addrinfo* res = NULL;
  struct addrinfo hints;

  ThriftUDPSocket *tsocket = THRIFT_UDP_SOCKET (transport);

  g_return_val_if_fail (tsocket->sd == THRIFT_INVALID_SOCKET, FALSE);

  // create the socket
  if ((tsocket->sd = socket (fam, SOCK_DGRAM, 0)) == -1)
  {
    g_set_error (error, THRIFT_TRANSPORT_ERROR, THRIFT_TRANSPORT_ERROR_SOCKET,
                 "failed to create socket for host %s:%d - %s",
                 tsocket->hostname, tsocket->port,
                 strerror(errno));
    return FALSE;
  }

  if (!tsocket->server) {

    g_message("Creating CLIENT socket");
    memset (&pin, 0, sizeof(pin));
    memset(&hints, 0, sizeof(struct addrinfo));
    
    hints.ai_family = fam;
    hints.ai_socktype = SOCK_DGRAM;

    getaddrinfo(tsocket->hostname, NULL, &hints, &res);

    memcpy(&result, res->ai_addr, res->ai_addrlen);

    // create a socket structure
#if defined(USE_IPV6)
    pin.sin6_addr = ((struct sockaddr_in6*) &result)->sin6_addr;
    pin.sin6_family = fam;
    pin.sin6_port = htons(tsocket->port); 
#else
    pin.sin_addr.s_addr = ((struct sockaddr_in*) &result)->sin_addr.s_addr;
    pin.sin_family = fam;
    pin.sin_port = htons(tsocket->port);
#endif
    memcpy(tsocket->conn_sock, &pin, sizeof(pin));
    tsocket->conn_size = sizeof(pin);

  } else {
    // g_message("Setting socket option");
    // Set the address to reusable so you don't have to wait after restarting
    if (setsockopt(tsocket->sd, SOL_SOCKET, SO_REUSEADDR, &enabled,
                   sizeof(enabled)) == -1) {
      g_set_error (error, THRIFT_TRANSPORT_ERROR,
                   THRIFT_TRANSPORT_ERROR_SOCKET,
                   "unable to set SO_REUSEADDR - %s", strerror(errno));
      return FALSE;
    }

    // g_message("Creating server socket (%d)", tsocket->sd);
    memset (&serveraddr, 0, sizeof(serveraddr));

    // g_message("Setting values with listen port: %d", tsocket->listen_port);
#if defined(USE_IPV6)
    serveraddr.sin6_addr = in6addr_any;
    serveraddr.sin6_family = fam;
    serveraddr.sin6_port = htons(tsocket->listen_port);
#else
    serveraddr.sin_addr.s_addr = INADDR_ANY;
    serveraddr.sin_family = fam;
    serveraddr.sin_port = htons(tsocket->listen_port);
#endif
    // g_message("Attempting to bind with size %d, fam: %d.", sizeof(serveraddr), serveraddr.sin6_family);
    // Bind to specified listen port
    if(bind(tsocket->sd, (struct sockaddr*) &serveraddr, sizeof(serveraddr)) < 0){
      g_set_error(error, THRIFT_TRANSPORT_ERROR, THRIFT_TRANSPORT_ERROR_SOCKET,
                  "failed to bind to server socket to port %d - %s",
                  tsocket->listen_port,
                  strerror(errno));
      return FALSE;
    }
  }

  g_message("returning true");
  return TRUE;
}

/* implements thrift_transport_close */
gboolean
thrift_udp_socket_close (ThriftTransport *transport, GError **error)
{
  ThriftUDPSocket *socket = THRIFT_UDP_SOCKET (transport);

  if (close (socket->sd) == -1)
  {
    g_set_error (error, THRIFT_TRANSPORT_ERROR, THRIFT_TRANSPORT_ERROR_CLOSE,
                 "unable to close socket - %s",
                 strerror(errno));
    return FALSE;
  }

  socket->sd = THRIFT_INVALID_SOCKET;
  return TRUE;
}

/* implements thrift_transport_read */
gint32
thrift_udp_socket_read (ThriftTransport *transport, gpointer buf,
                    guint32 len, GError **error)
{
#if defined(USE_IPV6)
  struct sockaddr_in6 src_addr;
#else
  struct sockaddr_in src_addr;
#endif
  socklen_t srcaddrlen = sizeof(src_addr);
  socklen_t addrlen = sizeof(src_addr);
  gint ret = 0;
  guint got = 0;

  ThriftUDPSocket *socket = THRIFT_UDP_SOCKET (transport);

  // TODO: do a MSG_PEEK to see the size of the packet before reading it off (avoids large buffer )

  // Create the message structure
  guchar buffer[len]; // buffer should only hold as much as the r_buf_size in transport

  struct iovec iov[1];
  iov[0].iov_base = buffer;
  iov[0].iov_len = sizeof(buffer);

  struct msghdr message;
  message.msg_name = &src_addr;
  message.msg_namelen = srcaddrlen;
  message.msg_iov = iov;
  message.msg_iovlen = 1;
  message.msg_control = 0;
  message.msg_controllen = 0;

  // Receive the entire message from the socket
  // g_message("Call recvmsg");
  ret = recvmsg (socket->sd, &message, 0);
  uint64_t t = getns();
  socket->recv_timestamp = g_array_append_val(socket->recv_timestamp, t);

  if (ret <= 0)
  {
    g_set_error (error, THRIFT_TRANSPORT_ERROR,
                 THRIFT_TRANSPORT_ERROR_RECEIVE,
                 "failed to read %d bytes - %s", len, strerror(errno));
    return -1;
  }
  got = ret;
  // TODO: got should always be less than len --> assert that? otherwise we will overflow
  // Copy the message into the buffer (which will be copied into the transport read buffer)
  memcpy(buf, buffer, (got < len) ? got : len);

  // g_message("Got back %d, wanted: %d", got, len);
  // print_n_bytes(buffer, got);

  // If our destination socket is not set, we should set it to whatever socket sent this info
  // g_message("family: %d", ((struct sockaddr*) socket->conn_sock)->sa_family);
  // if (((struct sockaddr*) socket->conn_sock)->sa_family == 0) {
    // g_message("Setting connection socket");
    memcpy(socket->conn_sock, &src_addr, addrlen);
    socket->conn_size = addrlen;
  // }

  // Return the TOTAL message size
  return got;
}

/* implements thrift_transport_read_end
 * called when read is complete.  nothing to do on our end. */
gboolean
thrift_udp_socket_read_end (ThriftTransport *transport, GError **error)
{
  /* satisfy -Wall */
  THRIFT_UNUSED_VAR (transport);
  THRIFT_UNUSED_VAR (error);
  return TRUE;
}

/* implements thrift_transport_write */
gboolean
thrift_udp_socket_write (ThriftTransport *transport, const gpointer buf,     
                     const guint32 len, GError **error)
{
  gint ret = 0;
  guint sent = 0;

  ThriftUDPSocket *socket = THRIFT_UDP_SOCKET (transport);
  g_return_val_if_fail (socket->sd != THRIFT_INVALID_SOCKET, FALSE);

  // If the sa_family is 0 that means the destination socket is never set
  if (((struct sockaddr*) socket->conn_sock)->sa_family == 0) {
    g_set_error (error, THRIFT_TRANSPORT_ERROR, THRIFT_TRANSPORT_ERROR_SEND,
                 "conn_sock is null");
  }

  while (sent < len)
  {
    // printf("Sending to: ");
    // print_n_bytes(socket->conn_sock->sin6_addr, 16);
    // printf("len: %d, sd: %d\n", len - sent, socket->sd);
    // Send to the destination sock (an object variable of the socket)
    ret = sendto (socket->sd, buf + sent, len - sent, 0, (struct sockaddr *) socket->conn_sock, socket->conn_size);
    if (ret < 0)
    {
      g_set_error (error, THRIFT_TRANSPORT_ERROR,
                   THRIFT_TRANSPORT_ERROR_SEND,
                   "failed to send %d bytes - %s", len, strerror(errno));
      // g_message("Sendto failed sending to family %d", ((struct sockaddr*) socket->conn_sock)->sa_family);
      return FALSE;
    }
    sent += ret;
  }

  uint64_t t = getns();
  socket->send_timestamp = g_array_append_val(socket->send_timestamp, t);

  return TRUE;
}

/* implements thrift_transport_write_end
 * called when write is complete.  nothing to do on our end. */
gboolean
thrift_udp_socket_write_end (ThriftTransport *transport, GError **error)
{
  /* satisfy -Wall */
  THRIFT_UNUSED_VAR (transport);
  THRIFT_UNUSED_VAR (error);
  return TRUE;
}

/* implements thrift_transport_flush
 * flush pending data.  since we are not buffered, this is a no-op */
gboolean
thrift_udp_socket_flush (ThriftTransport *transport, GError **error)
{
  /* satisfy -Wall */
  THRIFT_UNUSED_VAR (transport);
  THRIFT_UNUSED_VAR (error);
  return TRUE;
}

gboolean
thrift_udp_socket_record_timestamps (ThriftTransport *transport, 
                                     FILE* out, ThriftSocketOperation op) {
  ThriftUDPSocket *socket = THRIFT_UDP_SOCKET(transport);
  int size = 0;
  GArray *arr;

  switch (op) {
    case THRIFT_PERF_RECV:
      size = socket->recv_timestamp->len;
      arr = socket->recv_timestamp;
      break;
    case THRIFT_PERF_SEND:
      size = socket->send_timestamp->len;
      arr = socket->send_timestamp;
      break;
    default:
      return FALSE;
  }

  for(int i = 0; i < size; i++) {
    fprintf(out, "%lu\n", g_array_index(arr, guint64, i));
  }

  return TRUE;
}

/* initializes the instance */
static void
thrift_udp_socket_init (ThriftUDPSocket *socket)
{
  socket->sd = THRIFT_INVALID_SOCKET;
#if defined(USE_IPV6)
  socket->conn_sock = (struct sockaddr_in6*)malloc(sizeof(struct sockaddr_in6));
  socket->conn_size = sizeof(struct sockaddr_in6);
#else
  socket->conn_sock = (struct sockaddr_in*)malloc(sizeof(struct sockaddr_in));
  socket->conn_size = sizeof(struct sockaddr_in);
#endif
  socket->recv_timestamp = g_array_new(FALSE, TRUE, sizeof(guint64));
  socket->send_timestamp = g_array_new(FALSE, TRUE, sizeof(guint64));
}

/* destructor */
static void
thrift_udp_socket_finalize (GObject *object)
{
  ThriftUDPSocket *socket = THRIFT_UDP_SOCKET (object);

  if (socket->hostname != NULL)
  {
    g_free (socket->hostname);
  }
  socket->hostname = NULL;

  if (socket->sd != THRIFT_INVALID_SOCKET)
  {
    close (socket->sd);
  }
  socket->sd = THRIFT_INVALID_SOCKET;

  if (socket->conn_sock != NULL) {
    free(socket->conn_sock);
  }
  socket->conn_size = 0;
  socket->conn_sock = NULL;

  g_array_free(socket->recv_timestamp, TRUE);
  g_array_free(socket->send_timestamp, TRUE);
}

// TODO: add buf size as a param
/* property accessor */
void
thrift_udp_socket_get_property (GObject *object, guint property_id,
                            GValue *value, GParamSpec *pspec)
{
  THRIFT_UNUSED_VAR (pspec);
  ThriftUDPSocket *socket = THRIFT_UDP_SOCKET (object);

  switch (property_id)
  {
    case PROP_THRIFT_UDP_SOCKET_HOSTNAME:
      g_value_set_string (value, socket->hostname);
      break;
    case PROP_THRIFT_UDP_SOCKET_PORT:
      g_value_set_uint (value, socket->port);
      break;
    case PROP_THRIFT_UDP_SOCKET_SERVER:
      g_value_set_boolean (value, socket->server);
      break;
    case PROP_THRIFT_UDP_SOCKET_LISTEN_PORT:
      g_value_set_uint (value, socket->listen_port);
      break;
  }
}

/* property mutator */
void
thrift_udp_socket_set_property (GObject *object, guint property_id,
                            const GValue *value, GParamSpec *pspec)
{
  THRIFT_UNUSED_VAR (pspec);
  ThriftUDPSocket *socket = THRIFT_UDP_SOCKET (object);

  switch (property_id)
  {
    case PROP_THRIFT_UDP_SOCKET_HOSTNAME:
      socket->hostname = g_strdup (g_value_get_string (value));
      break;
    case PROP_THRIFT_UDP_SOCKET_PORT:
      socket->port = g_value_get_uint (value);
      break;
    case PROP_THRIFT_UDP_SOCKET_SERVER:
      socket->server = g_value_get_boolean(value);
      break;
    case PROP_THRIFT_UDP_SOCKET_LISTEN_PORT:
      socket->listen_port = g_value_get_uint(value);
      break;
  }
}

/* initializes the class */
static void
thrift_udp_socket_class_init (ThriftUDPSocketClass *cls)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (cls);
  GParamSpec *param_spec = NULL;

  /* setup accessors and mutators */
  gobject_class->get_property = thrift_udp_socket_get_property;
  gobject_class->set_property = thrift_udp_socket_set_property;

  param_spec = g_param_spec_string ("hostname",
                                    "hostname (construct)",
                                    "Set the hostname of the remote host",
                                    "localhost", /* default value */
                                    G_PARAM_CONSTRUCT_ONLY |
                                    G_PARAM_READWRITE);
  g_object_class_install_property (gobject_class, PROP_THRIFT_UDP_SOCKET_HOSTNAME,
                                   param_spec);

  param_spec = g_param_spec_uint ("port",
                                  "port (construct)",
                                  "Set the port of the remote host",
                                  0, /* min */
                                  65534, /* max */
                                  9090, /* default by convention */
                                  G_PARAM_CONSTRUCT_ONLY |
                                  G_PARAM_READWRITE);
  g_object_class_install_property (gobject_class, PROP_THRIFT_UDP_SOCKET_PORT,
                                   param_spec);

  param_spec = g_param_spec_uint ("listen_port",
                                  "port (construct)",
                                  "Set the listen port of the local host",
                                  0, /* min */
                                  65534, /* max */
                                  9090, /* default by convention */
                                  G_PARAM_CONSTRUCT_ONLY |
                                  G_PARAM_READWRITE);
  g_object_class_install_property (gobject_class, PROP_THRIFT_UDP_SOCKET_LISTEN_PORT,
                                   param_spec);

  param_spec = g_param_spec_boolean ("server",
                                  "server (construct)",
                                  "If true, this was created by the server",
                                  FALSE, /* default by convention */
                                  G_PARAM_CONSTRUCT_ONLY |
                                  G_PARAM_READWRITE);
  g_object_class_install_property (gobject_class, PROP_THRIFT_UDP_SOCKET_SERVER,
                                   param_spec);

  ThriftTransportClass *ttc = THRIFT_TRANSPORT_CLASS (cls);

  gobject_class->finalize = thrift_udp_socket_finalize;
  ttc->is_open = thrift_udp_socket_is_open;
  ttc->peek = thrift_udp_socket_peek;
  ttc->open = thrift_udp_socket_open;
  ttc->close = thrift_udp_socket_close;
  ttc->read = thrift_udp_socket_read;
  ttc->read_end = thrift_udp_socket_read_end;
  ttc->write = thrift_udp_socket_write;
  ttc->write_end = thrift_udp_socket_write_end;
  ttc->flush = thrift_udp_socket_flush;
  ttc->record_timestamps = thrift_udp_socket_record_timestamps;
}
