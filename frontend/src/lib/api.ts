import {
  TaskState,
  TaskStateSchema,
  TaskSchema,
  ViewTasksRequestSchema,
  ViewTasksResponseSchema,
  SubmitTaskRequestSchema,
  SubmitTaskResponseSchema,
  CancelTasksRequestSchema,
  CancelTasksResponseSchema,
  CancelTasksRequest_SelectorSchema,
  GetStatsRequestSchema,
  GetStatsResponseSchema,
  Task_TaskPriority,
  type Task,
  type GetStatsResponse_Stats,
  type CancelTasksRequest_Selector
} from './proto/multiqueue_pb';
import { create, fromJson, fromJsonString, toJsonString } from '@bufbuild/protobuf';

const API_BASE_URL = 'http://0.0.0.0:3000';

export async function fetchTasks(stateFilter: TaskState, limit: number = 20): Promise<Task[]> {
  try {
    const request = create(ViewTasksRequestSchema, {
      stateFilter: stateFilter,
      limit: limit
    });

    const json = await fetch(`${API_BASE_URL}/tasks/view`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: toJsonString(ViewTasksRequestSchema, request)
    });

    if (!json.ok) {
      throw new Error(`API error: ${json.status}`);
    }

    const data = await json.json();
    const response = fromJson(ViewTasksResponseSchema, data);

    if (response.response?.case === 'success') {
      return response.response.value.tasks;
    }

    return [];
  } catch (error) {
    console.error('Failed to fetch tasks:', error);
    throw error;
  }
}

export async function submitTask(name: string, priority: number = 0, tier: number = 3): Promise<Task | null> {
  try {
    const request = create(SubmitTaskRequestSchema, {
      name,
      priority: priority as Task_TaskPriority,
      tier
    });

    const json = await fetch(`${API_BASE_URL}/tasks/submit`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: toJsonString(SubmitTaskRequestSchema, request)
    });

    if (!json.ok) {
      throw new Error(`API error: ${json.status}`);
    }

    const data = await json.json();
    const response = fromJson(SubmitTaskResponseSchema, data);

    if (response.response?.case === 'success') {
      return response.response.value;
    }

    return null;
  } catch (error) {
    console.error('Failed to submit task:', error);
    throw error;
  }
}

export async function fetchStats(tier?: number): Promise<GetStatsResponse_Stats | null> {
  try {
    // Create request with optional tier parameter
    const request = create(GetStatsRequestSchema, { tier });

    const json = await fetch(`${API_BASE_URL}/tasks/stats`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: toJsonString(GetStatsRequestSchema, request)
    });

    if (!json.ok) {
      throw new Error(`API error: ${json.status}`);
    }

    const data = await json.json();
    const response = fromJson(GetStatsResponseSchema, data);

    if (response.response?.case === 'success') {
      return response.response.value;
    }

    return null;
  } catch (error) {
    console.error('Failed to fetch stats:', error);
    throw error;
  }
}

/**
 * Cancel a task by name
 * @param taskName The name of the task to cancel
 * @returns Number of tasks cancelled (should be 1 if successful)
 */
export async function cancelTask(taskName: string): Promise<number | null> {
  try {
    // Create a selector using the proper schema
    const selector = create(CancelTasksRequest_SelectorSchema, {
      selector: {
        case: "name",
        value: taskName
      }
    });

    const request = create(CancelTasksRequestSchema, {
      selectors: [selector]
    });

    // Call the cancel API endpoint
    const json = await fetch(`${API_BASE_URL}/tasks/cancel`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: toJsonString(CancelTasksRequestSchema, request)
    });

    if (!json.ok) {
      throw new Error(`API error: ${json.status}`);
    }

    const data = await json.json();
    const response = fromJson(CancelTasksResponseSchema, data);

    if (response.response?.case === 'count') {
      return Number(response.response.value);
    }

    return null;
  } catch (error) {
    console.error('Failed to cancel task:', error);
    throw error;
  }
}
