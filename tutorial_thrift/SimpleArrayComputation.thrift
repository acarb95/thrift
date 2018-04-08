include "shared.thrift"

struct tuple {
	1:i32 n,
	2:i32 m,
}

service SimpleArrayComputation {
	/**
	 * Request the server to increment all values in a region
	 * Server returns a new memory region of the incremented values
	 */
	list<byte> increment_array(1:list<byte> arr, 2:byte value, 3:i32 length) throws (1:shared.CallException ouch),

	/**
	 * Add two arrays and return a pointer to the result.
	 * Returns an exception if the lengths do not match
	 */
	list<byte> add_arrays(1:list<byte> array1, 2:list<byte> array2, 3:i32 length) throws (1:shared.CallException ouch),

	/**
	 * Multiply an array and matrix. Store the result in result_ptr. 
	 * Will throw an exception if the dimension and length of the matrix and array do not match
	 */
	list<byte> mat_multiply(1:list<byte> array, 2:list<list<byte>> matrix, 3:i32 length, 4:tuple dimension) throws (1:shared.CallException ouch),

	/**
	 * Gets the word count for a specific pointer and length
	 * Will throw an exception if the pointer is invalid
	 */
	i32 word_count(1:string story, 2:i32 length) throws (1:shared.CallException ouch),

	/**
	 * Sort a number array, return the sorted array
	 */
	list<byte> sort_array(1:list<byte> num_array, 2:i32 length) throws (1:shared.CallException ouch),

	/**
	 * Shared pointer no-op. Passes a shared argument and shared result with no operation.
	 * Client side performs 1 allocate, 1 write, and 1 read.
	 * Server side performs 1 allocate, 1 read, and 1 write. 
	 */
	list<byte> no_op(1:list<byte> num_array, 2:i32 length),
}