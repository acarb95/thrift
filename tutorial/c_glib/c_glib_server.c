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

#include <glib-object.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <unistd.h>
#include <stdlib.h>

#include <thrift/c_glib/thrift.h>
#include <thrift/c_glib/protocol/thrift_binary_protocol_factory.h>
#include <thrift/c_glib/protocol/thrift_protocol_factory.h>
#include <thrift/c_glib/server/thrift_server.h>
#include <thrift/c_glib/server/thrift_simple_server.h>
#include <thrift/c_glib/transport/thrift_buffered_udp_transport_factory.h>
#include <thrift/c_glib/transport/thrift_server_udp_socket.h>
#include <thrift/c_glib/transport/thrift_server_transport.h>

#include "gen-c_glib/shared_memory_test.h"
#include "../lib/client_lib.h"
#include "../lib/config.h"
#include "../lib/utils.h"

G_BEGIN_DECLS

/* In the C (GLib) implementation of Thrift, the actual work done by a
   server---that is, the code that runs when a client invokes a
   service method---is defined in a separate "handler" class that
   implements the service interface. Here we define the
   TutorialSharedMemoryTestHandler class, which implements the SharedMemoryTestIf
   interface and provides the behavior expected by tutorial clients.
   (Typically this code would be placed in its own module but for
   clarity this tutorial is presented entirely in a single file.)

   For each service the Thrift compiler generates an abstract base
   class from which handler implementations should inherit. In our
   case TutorialSharedMemoryTestHandler inherits from SharedMemoryTestHandler,
   defined in gen-c_glib/shared_memory_test.h.

   If you're new to GObject, try not to be intimidated by the quantity
   of code here---much of it is boilerplate and can mostly be
   copied-and-pasted from existing work. For more information refer to
   the GObject Reference Manual, available online at
   https://developer.gnome.org/gobject/. */

#define TYPE_TUTORIAL_SHARED_MEMORY_TEST_HANDLER \
  (tutorial_shared_memory_test_handler_get_type ())

#define TUTORIAL_SHARED_MEMORY_TEST_HANDLER(obj)                                \
  (G_TYPE_CHECK_INSTANCE_CAST ((obj),                                   \
                               TYPE_TUTORIAL_SHARED_MEMORY_TEST_HANDLER,        \
                               TutorialSharedMemoryTestHandler))
#define TUTORIAL_SHARED_MEMORY_TEST_HANDLER_CLASS(c)                    \
  (G_TYPE_CHECK_CLASS_CAST ((c),                                \
                            TYPE_TUTORIAL_SHARED_MEMORY_TEST_HANDLER,   \
                            TutorialSharedMemoryTestHandlerClass))
#define IS_TUTORIAL_SHARED_MEMORY_TEST_HANDLER(obj)                             \
  (G_TYPE_CHECK_INSTANCE_TYPE ((obj),                                   \
                               TYPE_TUTORIAL_SHARED_MEMORY_TEST_HANDLER))
#define IS_TUTORIAL_SHARED_MEMORY_TEST_HANDLER_CLASS(c)                 \
  (G_TYPE_CHECK_CLASS_TYPE ((c),                                \
                            TYPE_TUTORIAL_SHARED_MEMORY_TEST_HANDLER))
#define TUTORIAL_SHARED_MEMORY_TEST_HANDLER_GET_CLASS(obj)              \
  (G_TYPE_INSTANCE_GET_CLASS ((obj),                            \
                              TYPE_TUTORIAL_SHARED_MEMORY_TEST_HANDLER, \
                              TutorialSharedMemoryTestHandlerClass))

struct _TutorialSharedMemoryTestHandler {
  SharedMemoryTestHandler parent_instance;

  /* private */
  GHashTable *log;
};
typedef struct _TutorialSharedMemoryTestHandler TutorialSharedMemoryTestHandler;

struct _TutorialSharedMemoryTestHandlerClass {
  SharedMemoryTestHandlerClass parent_class;
};
typedef struct _TutorialSharedMemoryTestHandlerClass TutorialSharedMemoryTestHandlerClass;

GType tutorial_shared_memory_test_handler_get_type (void);

G_END_DECLS

struct sockaddr_in6 *targetIP;

/* ---------------------------------------------------------------- */

/* The implementation of TutorialSharedMemoryTestHandler follows. */

void get_result_pointer(struct in6_memaddr *ptr) {
  // Get random memory server
  struct in6_addr *ipv6Pointer = gen_ip6_target(0);

  // Put it's address in targetIP (why?)
  memcpy(&(targetIP->sin6_addr), ipv6Pointer, sizeof(*ipv6Pointer));

  // Allocate memory and receive the remote memory pointer
  struct in6_memaddr temp = allocate_rmem(targetIP);

  // Copy the remote memory pointer into the give struct pointer
  memcpy(ptr, &temp, sizeof(struct in6_memaddr));
}

void marshall_shmem_ptr(GByteArray **ptr, struct in6_memaddr *addr) {
  // Blank cmd section
  uint16_t cmd = 0u;

  // Copy wildcard (::)
  *ptr = g_byte_array_append(*ptr, (const gpointer) &(addr->wildcard), sizeof(uint32_t));
  // Copy subid (i.e., 103)
  *ptr = g_byte_array_append(*ptr, (const gpointer) &(addr->subid), sizeof(uint16_t));
  // Copy cmd (0)
  *ptr = g_byte_array_append(*ptr, (const gpointer) &cmd, sizeof(uint16_t));
  // Copy memory address (XXXX:XXXX)
  *ptr = g_byte_array_append(*ptr, (const gpointer) &(addr->paddr), sizeof(uint64_t));
}

void unmarshall_shmem_ptr(struct in6_memaddr *result_addr, GByteArray *result_ptr) {
  // Clear struct
  memset(result_addr, 0, sizeof(struct in6_memaddr));
  // Copy over received bytes
  memcpy(result_addr, result_ptr->data, sizeof(struct in6_memaddr));
}


G_DEFINE_TYPE (TutorialSharedMemoryTestHandler,
               tutorial_shared_memory_test_handler,
               TYPE_SHARED_MEMORY_TEST_HANDLER);

static gboolean
tutorial_shared_memory_test_handler_ping (SharedMemoryTestIf  *iface,
                                 GError              **error)
{
  THRIFT_UNUSED_VAR (iface);
  THRIFT_UNUSED_VAR (error);

  puts ("ping()");

  return TRUE;
}

static gboolean
tutorial_shared_memory_test_handler_allocate_mem (SharedMemoryTestIf *iface,
                                                  GByteArray        **_return,
                                                  const gint32        size,
                                                  CallException     **ouch,
                                                  GError            **error)
{
  THRIFT_UNUSED_VAR (iface);
  THRIFT_UNUSED_VAR (error);
  THRIFT_UNUSED_VAR (ouch);
  THRIFT_UNUSED_VAR (_return);
  THRIFT_UNUSED_VAR (size);

  // TODO: change so it allocates a certain amount of memory

  GByteArray *result_ptr = g_byte_array_new();
  struct in6_memaddr result_addr;

  get_result_pointer(&result_addr);

  marshall_shmem_ptr(&result_ptr, &result_addr);

  g_byte_array_ref(result_ptr); // Increase the reference count so it doesn't get garbage collected

  *_return = result_ptr;

  printf("allocate_mem(): returning ");
  print_n_bytes(result_ptr->data, result_ptr->len);

  return TRUE;

}

static gboolean
tutorial_shared_memory_test_handler_read_mem (SharedMemoryTestIf *iface,
                                              const GByteArray   *pointer,
                                              CallException     **ouch,
                                              GError            **error)
{
  THRIFT_UNUSED_VAR (iface);
  THRIFT_UNUSED_VAR (error);
  THRIFT_UNUSED_VAR (ouch);

  char *payload = malloc(4096);

  struct in6_memaddr args_addr;
  unmarshall_shmem_ptr(&args_addr, (GByteArray *)pointer);

  get_rmem(payload, 4096, targetIP, &args_addr);

  printf("read_mem(): \"%s\"\n", payload);

  free(payload);
  return TRUE;

}

static gboolean
tutorial_shared_memory_test_handler_write_mem (SharedMemoryTestIf *iface,
                                               const GByteArray   *pointer,
                                               const gchar        *message,
                                               CallException     **ouch,
                                               GError            **error) 
{
  THRIFT_UNUSED_VAR (iface);
  THRIFT_UNUSED_VAR (error);
  THRIFT_UNUSED_VAR (ouch);

  printf ("write_mem(%s)\n", message);

  struct in6_memaddr args_addr;
  unmarshall_shmem_ptr(&args_addr, (GByteArray *) pointer);

  write_rmem(targetIP, (char *) message, &args_addr);

  return TRUE;

}

static gboolean
tutorial_shared_memory_test_handler_free_mem (SharedMemoryTestIf *iface,
                                              const GByteArray   *pointer, 
                                              CallException     **ouch, 
                                              GError            **error) 
{
  THRIFT_UNUSED_VAR (iface);
  THRIFT_UNUSED_VAR (error);
  THRIFT_UNUSED_VAR (ouch);

  printf ("free_mem: ");
  print_n_bytes(pointer->data, pointer->len);

  struct in6_memaddr args_addr;
  unmarshall_shmem_ptr(&args_addr, (GByteArray *) pointer);

  free_rmem(targetIP, &args_addr);

  return TRUE;

}

static gboolean
tutorial_shared_memory_test_handler_increment_array (SharedMemoryTestIf *iface,
                                                   GByteArray        **_return,
                                                   const GByteArray   *pointer,
                                                   const gint8        value,
                                                   const gint32        length,
                                                   CallException     **ouch,
                                                   GError            **error) 
{
  THRIFT_UNUSED_VAR (iface);
  THRIFT_UNUSED_VAR (error);
  THRIFT_UNUSED_VAR (ouch);

  GByteArray* result_ptr = g_byte_array_new();
  struct in6_memaddr result_addr;

  get_result_pointer(&result_addr);

  marshall_shmem_ptr(&result_ptr, &result_addr);

  // Read in array from shared memory
  uint8_t *int_arr = malloc(length);

  struct in6_memaddr args_addr;
  unmarshall_shmem_ptr(&args_addr, (GByteArray *) pointer);

  get_rmem((char *) int_arr, length, targetIP, &args_addr);

  // Increment the values
  for (int i = 0; i < length; i++) {
    int_arr[i] += value;
  }

  // Write it to the array
  write_rmem(targetIP, (char*) int_arr, &result_addr);

  g_byte_array_ref(result_ptr);

  *_return = result_ptr;

  printf("increment_array() returning ");
  print_n_bytes(result_ptr->data, result_ptr->len);

  free(int_arr);
  return TRUE;

}

static gboolean
tutorial_shared_memory_test_handler_add_arrays (SharedMemoryTestIf *iface,
                                                GByteArray        **_return,
                                                const GByteArray   *array1,
                                                const GByteArray   *array2,
                                                const gint32        length,
                                                CallException     **ouch,
                                                GError            **error)
{
  THRIFT_UNUSED_VAR (iface);
  THRIFT_UNUSED_VAR (error);
  THRIFT_UNUSED_VAR (ouch);

  GByteArray* result_ptr = g_byte_array_new();
  struct in6_memaddr result_addr;

  get_result_pointer(&result_addr);

  marshall_shmem_ptr(&result_ptr, &result_addr);

  // Read in params from shared memory
  uint8_t *arr1 = malloc(length);
  uint8_t *arr2 = malloc(length);

  struct in6_memaddr arg1_addr;
  struct in6_memaddr arg2_addr;

  unmarshall_shmem_ptr(&arg1_addr, (GByteArray *) array1);
  unmarshall_shmem_ptr(&arg2_addr, (GByteArray *) array2);

  get_rmem((char*) arr1, length, targetIP, &arg1_addr);
  get_rmem((char*) arr2, length, targetIP, &arg2_addr);

  // Create result array
  uint8_t *result_array = malloc(length);

  // Perform computation
  for (int i = 0; i < length; i++) {
    result_array[i] = arr1[i] + arr2[i];
  }

  // Write computation to shared memory
  write_rmem(targetIP, (char*) result_array, &result_addr);
  g_byte_array_ref(result_ptr);

  printf ("add_arrays (%d): \n\t1: ", length);
  print_n_bytes(array1->data, array1->len);
  printf("\t2: ");
  print_n_bytes(array2->data, array2->len);

  *_return = result_ptr;

  free(result_array);
  return TRUE;

}

static gboolean
tutorial_shared_memory_test_handler_mat_multiply (SharedMemoryTestIf *iface,
                                                  const GByteArray   *array,
                                                  const GByteArray   *matrix,
                                                  const gint32        length,
                                                  const tuple        *dimension,
                                                  const GByteArray   *result_ptr,
                                                  CallException     **ouch,
                                                  GError            **error)
{
  THRIFT_UNUSED_VAR (iface);
  THRIFT_UNUSED_VAR (error);
  THRIFT_UNUSED_VAR (ouch);
  THRIFT_UNUSED_VAR (result_ptr);

  printf ("mat_multiply (length: %d, dimension: %d, %d): \n\tArray: ", length, dimension->n, dimension->m);
  print_n_bytes(array->data, array->len);
  printf("\tMatrix: ");
  print_n_bytes(matrix->data, matrix->len);

  return TRUE;

}

static gboolean
tutorial_shared_memory_test_handler_word_count (SharedMemoryTestIf *iface,
                                                gint32             *_return,
                                                const GByteArray   *story,
                                                const gint32        length,
                                                CallException     **ouch,
                                                GError            **error)
{
  THRIFT_UNUSED_VAR (iface);
  THRIFT_UNUSED_VAR (error);
  THRIFT_UNUSED_VAR (ouch);

  *_return = -1;

  printf("word_count (%d): ", length);
  print_n_bytes(story->data, story->len);

  return TRUE;

}

static gboolean
tutorial_shared_memory_test_handler_sort_array (SharedMemoryTestIf *iface,
                                                GByteArray        **_return,
                                                const GByteArray   *num_array,
                                                const gint32        length,
                                                CallException     **ouch,
                                                GError            **error)
{
  THRIFT_UNUSED_VAR (iface);
  THRIFT_UNUSED_VAR (error);
  THRIFT_UNUSED_VAR (ouch);

  GByteArray* res = g_byte_array_new();

  g_byte_array_ref(res);

  *_return = res;

  printf("sort_array(%d): ", length);
  print_n_bytes(num_array->data, num_array->len);

  return TRUE;

}

// static gboolean
// tutorial_shared_memory_test_handler_sort_by (SharedMemoryTestIf  *iface,
//                                              GByteArray         **_return,
//                                              const GByteArray    *num_array,
//                                              const gint32         length,
//                                              const GByteArray    *comparator,
//                                              CallException      **ouch,
//                                              GError             **error)
// {
//   THRIFT_UNUSED_VAR (iface);
//   THRIFT_UNUSED_VAR (error);
//   THRIFT_UNUSED_VAR (ouch);

//   GByteArray* res = g_byte_array_new();

//   g_byte_array_ref(res);

//   *_return = res;

//   printf ("sort_by(%d), array: ", length);
//   print_n_bytes(num_array->data, num_array->len);
//   printf("comparator: ");
//   print_n_bytes(comparator->data, comparator->len);

//   return TRUE;

// }

/* TutorialSharedMemoryTestHandler's instance finalizer (destructor) */
static void
tutorial_shared_memory_test_handler_finalize (GObject *object)
{
  TutorialSharedMemoryTestHandler *self =
    TUTORIAL_SHARED_MEMORY_TEST_HANDLER (object);

  /* Free our calculation-log hash table */
  g_hash_table_unref (self->log);
  self->log = NULL;

  /* Chain up to the parent class */
  G_OBJECT_CLASS (tutorial_shared_memory_test_handler_parent_class)->
    finalize (object);
}

/* TutorialSharedMemoryTestHandler's instance initializer (constructor) */
static void
tutorial_shared_memory_test_handler_init (TutorialSharedMemoryTestHandler *self)
{
  /* Create our calculation-log hash table */
  self->log = g_hash_table_new_full (g_int_hash,
                                     g_int_equal,
                                     g_free,
                                     g_object_unref);
}

/* TutorialSharedMemoryTestHandler's class initializer */
static void
tutorial_shared_memory_test_handler_class_init (TutorialSharedMemoryTestHandlerClass *klass)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (klass);
  SharedMemoryTestHandlerClass *shared_memory_test_handler_class =
    SHARED_MEMORY_TEST_HANDLER_CLASS (klass);

  /* Register our destructor */
  gobject_class->finalize = tutorial_shared_memory_test_handler_finalize;

  /* Register our implementations of SharedMemoryTestHandler's methods */
  shared_memory_test_handler_class->ping =
    tutorial_shared_memory_test_handler_ping;
  shared_memory_test_handler_class->allocate_mem = 
    tutorial_shared_memory_test_handler_allocate_mem;
  shared_memory_test_handler_class->read_mem = 
    tutorial_shared_memory_test_handler_read_mem;
  shared_memory_test_handler_class->write_mem = 
    tutorial_shared_memory_test_handler_write_mem;
  shared_memory_test_handler_class->increment_array = 
    tutorial_shared_memory_test_handler_increment_array;
  shared_memory_test_handler_class->free_mem = 
    tutorial_shared_memory_test_handler_free_mem;
  shared_memory_test_handler_class->add_arrays = 
    tutorial_shared_memory_test_handler_add_arrays;
  shared_memory_test_handler_class->mat_multiply = 
    tutorial_shared_memory_test_handler_mat_multiply;
  shared_memory_test_handler_class->word_count = 
    tutorial_shared_memory_test_handler_word_count;
  shared_memory_test_handler_class->sort_array = 
    tutorial_shared_memory_test_handler_sort_array;
  // shared_memory_test_handler_class->sort_by = 
  //   tutorial_shared_memory_test_handler_sort_by;
}

/* ---------------------------------------------------------------- */

/* That ends the implementation of TutorialSharedMemoryTestHandler.
   Everything below is fairly generic code that sets up a minimal
   Thrift server for tutorial clients. */


/* Our server object, declared globally so it is accessible within the
   SIGINT signal handler */
ThriftServer *server = NULL;

/* A flag that indicates whether the server was interrupted with
   SIGINT (i.e. Ctrl-C) so we can tell whether its termination was
   abnormal */
gboolean sigint_received = FALSE;

/* Handle SIGINT ("Ctrl-C") signals by gracefully stopping the
   server */
static void
sigint_handler (int signal_number)
{
  THRIFT_UNUSED_VAR (signal_number);

  /* Take note we were called */
  sigint_received = TRUE;

  /* Shut down the server gracefully */
  if (server != NULL)
    thrift_server_stop (server);
}

int main (int argc, char *argv[])
{
  if (argc < 3) {
    printf("usage\n");
    return -1;
  }
  int c; 
  struct config myConf;
  while ((c = getopt (argc, argv, "c:")) != -1) { 
  switch (c) 
    { 
    case 'c':
      myConf = set_bb_config(optarg, 0);
      break;
    case '?': 
        fprintf (stderr, "Unknown option `-%c'.\n", optopt);
        printf("usage: -c config\n");
      return 1; 
    default:
      printf("usage\n");
      return -1;
    } 
  } 
  targetIP = init_sockets(&myConf, 0);
  set_host_list(myConf.hosts, myConf.num_hosts);

  TutorialSharedMemoryTestHandler *handler;
  SharedMemoryTestProcessor *processor;

  ThriftServerTransport *server_transport;
  ThriftTransportFactory *transport_factory;
  ThriftProtocolFactory *protocol_factory;

  struct sigaction sigint_action;

  GError *error;
  int exit_status = 0;

#if (!GLIB_CHECK_VERSION (2, 36, 0))
  g_type_init ();
#endif

  /* Create an instance of our handler, which provides the service's
     methods' implementation */
  handler =
    g_object_new (TYPE_TUTORIAL_SHARED_MEMORY_TEST_HANDLER,
                  NULL);

  /* Create an instance of the service's processor, automatically
     generated by the Thrift compiler, which parses incoming messages
     and dispatches them to the appropriate method in the handler */
  processor =
    g_object_new (TYPE_SHARED_MEMORY_TEST_PROCESSOR,
                  "handler", handler,
                  NULL);

  /* Create our server socket, which binds to the specified port and
     listens for client connections */
  server_transport =
    g_object_new (THRIFT_TYPE_SERVER_UDP_SOCKET,
                  "port", 9090,
                  NULL);

  /* Create our transport factory, used by the server to wrap "raw"
     incoming connections from the client (in this case with a
     ThriftBufferedUDPTransport to improve performance) */
  transport_factory =
    g_object_new (THRIFT_TYPE_BUFFERED_UDP_TRANSPORT_FACTORY,
                  NULL);

  /* Create our protocol factory, which determines which wire protocol
     the server will use (in this case, Thrift's binary protocol) */
  protocol_factory =
    g_object_new (THRIFT_TYPE_BINARY_PROTOCOL_FACTORY,
                  NULL);

  /* Create the server itself */
  server =
    g_object_new (THRIFT_TYPE_SIMPLE_SERVER,
                  "processor",                processor,
                  "server_transport",         server_transport,
                  "input_transport_factory",  transport_factory,
                  "output_transport_factory", transport_factory,
                  "input_protocol_factory",   protocol_factory,
                  "output_protocol_factory",  protocol_factory,
                  NULL);

  /* Install our SIGINT handler, which handles Ctrl-C being pressed by
     stopping the server gracefully (not strictly necessary, but a
     nice touch) */
  memset (&sigint_action, 0, sizeof (sigint_action));
  sigint_action.sa_handler = sigint_handler;
  sigint_action.sa_flags = SA_RESETHAND;
  sigaction (SIGINT, &sigint_action, NULL);

  /* Start the server, which will run until its stop method is invoked
     (from within the SIGINT handler, in this case) */
  puts ("Starting the server...");
  thrift_server_serve (server, &error);

  /* If the server stopped for any reason other than having been
     interrupted by the user, report the error */
  if (!sigint_received) {
    g_message ("thrift_server_serve: %s",
               error != NULL ? error->message : "(null)");
    g_clear_error (&error);
  }

  puts ("done.");

  g_object_unref (server);
  g_object_unref (transport_factory);
  g_object_unref (protocol_factory);
  g_object_unref (server_transport);

  g_object_unref (processor);
  g_object_unref (handler);

  return exit_status;
}
