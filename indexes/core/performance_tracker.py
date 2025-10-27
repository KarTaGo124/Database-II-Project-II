import time

class OperationResult:
    def __init__(self, data, execution_time_ms, disk_reads, disk_writes, rebuild_triggered=False, operation_breakdown=None):
        self.data = data
        self.execution_time_ms = execution_time_ms
        self.disk_reads = disk_reads
        self.disk_writes = disk_writes
        self.total_disk_accesses = disk_reads + disk_writes
        self.rebuild_triggered = rebuild_triggered
        self.operation_breakdown = operation_breakdown or {}

    def __repr__(self):
        rebuild_info = " [REBUILD]" if self.rebuild_triggered else ""
        breakdown_info = f" breakdown={self.operation_breakdown}" if self.operation_breakdown else ""
        return f"OperationResult(data={self.data}, time={self.execution_time_ms:.2f}ms, accesses={self.total_disk_accesses}{rebuild_info}{breakdown_info})"

class PerformanceTracker:
    def __init__(self):
        self.reset()

    def reset(self):
        self.reads = 0
        self.writes = 0
        self.start_time = 0
        self.operation_stack = []
        self.rebuild_occurred = False

    def start_operation(self):
        if self.start_time != 0:
            self.operation_stack.append({
                'reads': self.reads,
                'writes': self.writes,
                'start_time': self.start_time,
                'rebuild_occurred': self.rebuild_occurred
            })
        else:
            self.reads = 0
            self.writes = 0
            self.rebuild_occurred = False

        self.start_time = time.time()

    def track_read(self):
        self.reads += 1

    def track_write(self):
        self.writes += 1

    def end_operation(self, result_data, rebuild_triggered=False):
        execution_time = (time.time() - self.start_time) * 1000

        if rebuild_triggered:
            self.rebuild_occurred = True

        if self.operation_stack:
            previous_state = self.operation_stack.pop()
            total_reads = previous_state['reads'] + self.reads
            total_writes = previous_state['writes'] + self.writes

            combined_rebuild = self.rebuild_occurred or previous_state['rebuild_occurred']

            self.reads = total_reads
            self.writes = total_writes
            self.start_time = previous_state['start_time']
            self.rebuild_occurred = combined_rebuild

            return OperationResult(result_data, execution_time, self.reads - previous_state['reads'], self.writes - previous_state['writes'], combined_rebuild)
        else:
            result = OperationResult(result_data, execution_time, self.reads, self.writes, self.rebuild_occurred)
            self.reset()
            return result