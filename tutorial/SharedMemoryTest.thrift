struct tuple {
	1:i32 n,
	2:i32 m,
}

exception CallException {
	1:i32 err_code,
	2:string message
}

service SharedMemoryTest {
	/**
	 * Test network connectivity with a ping
	 */
	void ping(),

	/**
	 * Request memory of a certain size
	 */
	binary allocate_mem(1:i32 size) throws (1:CallException ouch),

	/**
	 * Ask the server to read memory from a pointer
	 */
	void read_mem(1:binary pointer) throws (1:CallException ouch),

	/**
	 * Request the server to write a message to a pointer
	 */
	void write_mem(1:binary pointer, 2:string message) throws (1:CallException ouch),

	/**
	 * Request the server to free memory
	 * Throws an exception if the server 
	 */
	void free_mem(1:binary pointer) throws (1:CallException ouch),

	/**
	 * Request the server to increment all values in a region
	 * Server returns a new memory region of the incremented values
	 */
	binary increment_array(1:binary pointer, 2:byte value, 3:i32 length) throws (1:CallException ouch),

	/**
	 * Add two arrays and return a pointer to the result.
	 * Returns an exception if the lengths do not match
	 */
	binary add_arrays(1:binary array1, 2:binary array2, 3:i32 length) throws (1:CallException ouch),

	/**
	 * Multiply an array and matrix. Store the result in result_ptr. 
	 * Will throw an exception if the dimension and length of the matrix and array do not match
	 */
	void mat_multiply(1:binary array, 2:binary matrix, 3:i32 length, 4:tuple dimension, 5:binary result_ptr) throws (1:CallException ouch),

	/**
	 * Gets the word count for a specific pointer and length
	 * Will throw an exception if the pointer is invalid
	 */
	i32 word_count(1:binary story, 2:i32 length) throws (1:CallException ouch),

	/**
	 * Sort a number array, return the sorted array
	 */
	binary sort_array(1:binary num_array, 2:i32 length) throws (1:CallException ouch),
}