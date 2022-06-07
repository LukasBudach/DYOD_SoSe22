#include "abstract_operator.hpp"

#include "assert.h"
#include "storage/reference_segment.hpp"

namespace opossum {

AbstractOperator::AbstractOperator(const std::shared_ptr<const AbstractOperator> left,
                                   const std::shared_ptr<const AbstractOperator> right)
    : _left_input(left), _right_input(right) {}

void AbstractOperator::execute() { _output = _on_execute(); }

std::shared_ptr<const Table> AbstractOperator::get_output() const {
  // ensure we return an empty chunk only if the chunk count is also one
  if (_output->get_chunk(ChunkID{_output->chunk_count() - ChunkID{1}})->size() == 0) {
    Assert(_output->chunk_count() == ChunkID{1},
           "Detected an empty chunk in an operator result with 2 or more chunks.");
  }

  return _output;
}

std::shared_ptr<const Table> AbstractOperator::_left_input_table() const { return _left_input->get_output(); }

std::shared_ptr<const Table> AbstractOperator::_right_input_table() const { return _right_input->get_output(); }

}  // namespace opossum
