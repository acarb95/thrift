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

#include <stdio.h>
#include <glib-object.h>
#include <inttypes.h>
#include <unistd.h>
#include <string.h>

#include <thrift/c_glib/protocol/thrift_binary_protocol.h>
#include <thrift/c_glib/transport/thrift_buffered_udp_transport.h>
#include <thrift/c_glib/transport/thrift_udp_socket.h>

#include "gen-c_glib/shared_memory_test.h"
#include "../lib/client_lib.h"
#include "../lib/config.h"
#include "../lib/utils.h"

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
  struct sockaddr_in6 *targetIP = init_sockets(&myConf, 0);
  set_host_list(myConf.hosts, myConf.num_hosts);

  ThriftUDPSocket *socket;
  ThriftTransport *transport;
  ThriftProtocol *protocol;
  SharedMemoryTestIf *client;

  GError *error = NULL;
  CallException *exception = NULL;

  int exit_status = 0;

#if (!GLIB_CHECK_VERSION (2, 36, 0))
  g_type_init ();
#endif

  socket    = g_object_new (THRIFT_TYPE_UDP_SOCKET,
                            "hostname",  "0:0:102::",
                            "port",      9090,
                            NULL);
  transport = g_object_new (THRIFT_TYPE_BUFFERED_UDP_TRANSPORT,
                            "transport", socket,
                            NULL);
  protocol  = g_object_new (THRIFT_TYPE_BINARY_PROTOCOL,
                            "transport", transport,
                            NULL);

  thrift_transport_open (transport, &error);

  client = g_object_new (TYPE_SHARED_MEMORY_TEST_CLIENT,
                         "input_protocol",  protocol,
                         "output_protocol", protocol,
                         NULL);

  puts("Testing ping...");
  // TEST: ping
  if (!error && shared_memory_test_if_ping (client, &error)) {
    puts ("\tping()");
  }

  puts("Testing allocate_mem...");
  // TEST: allocate_mem
  GByteArray* res = NULL;
  if (!error && shared_memory_test_if_allocate_mem(client, &res, 4096, &exception, &error)) {
    printf("\tReceived: ");
    print_n_bytes(res->data, res->len);
  }

  // TEST: write_mem
  puts("Testing write_mem...");

  // Clear payload
  char *payload = malloc(4096);
  snprintf(payload, 50, "HELLO WORLD! How are you?");

  if (!error && shared_memory_test_if_write_mem(client, res, payload, &exception, &error)) {
    puts ("\twrite_mem()");
  }

  // TEST: read_mem
  puts("Testing read_mem...");

  if (!error && shared_memory_test_if_read_mem(client, res, &exception, &error)) {
    puts ("\tread_mem()");
  }

  // TEST: increment_mem
  puts("Testing increment_mem...");
  // TODO: put into it's own utils function
  // Get server to get memory from.
  struct in6_addr *ipv6Pointer = gen_ip6_target(0);
  memcpy(&(targetIP->sin6_addr), ipv6Pointer, sizeof(*ipv6Pointer));
  struct in6_memaddr temp = allocate_rmem(targetIP);

  // Insert int array
  char *int_payload = malloc(4096);
  int arr[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  memcpy(int_payload, arr, 10);

  write_rmem(targetIP, int_payload, &temp);
  
  // TODO: put into it's own utils function
  // Marshal shared pointer address
  GByteArray* test = g_byte_array_new();
  uint16_t cmd = 0u;

  test = g_byte_array_append(test, (const gpointer) &(temp.wildcard), sizeof(uint32_t));
  test = g_byte_array_append(test, (const gpointer) &(temp.subid), sizeof(uint16_t));
  test = g_byte_array_append(test, (const gpointer) &cmd, sizeof(uint16_t));
  test = g_byte_array_append(test, (const gpointer) &(temp.paddr), sizeof(uint64_t));

  GByteArray* res2 = NULL;

  if (!error && shared_memory_test_if_increment_mem(client, &res2, test, 1, 10, &exception, &error)) {
    puts ("\tincrement_mem()");
  }

  puts("Testing free_mem...");
  // TEST: free_mem
  if (!error && shared_memory_test_if_free_mem(client, res, &exception, &error)) {
    puts ("\tfree_mem()");
  }

  if (error) {
    printf ("ERROR: %s\n", error->message);
    g_clear_error (&error);

    exit_status = 1;
  }

  thrift_transport_close (transport, NULL);

  g_object_unref (client);
  g_object_unref (protocol);
  g_object_unref (transport);
  g_object_unref (socket);

  return exit_status;
}
