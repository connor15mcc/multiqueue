<script lang="ts">
	import { type Task, TaskState } from '../lib/proto/multiqueue_pb';
	import { formatDistanceToNow } from 'date-fns';
	import { cancelTask } from '../lib/api';
	import { createEventDispatcher } from 'svelte';

	export let task: Task;

	const dispatch = createEventDispatcher();
	let isHovering = false;
	let cancelling = false;

	// Check if task is in waiting state
	$: isCancelable = task.state === TaskState.WAITING || task.state === TaskState.QUEUED;

	// Handle cancel button click
	async function handleCancel() {
		if (cancelling) return;

		try {
			cancelling = true;
			const result = await cancelTask(task.name);

			if (result && result > 0) {
				// Notify parent components that task was cancelled
				dispatch('taskCancelled', { taskName: task.name });
			}
		} catch (err) {
			console.error(`Failed to cancel task ${task.name}:`, err);
		} finally {
			cancelling = false;
		}
	}

	// Format timestamp as relative time using date-fns
	function formatRelativeTime(timestamp: bigint): string {
		const date = new Date(Number(timestamp) * 1000);
		return formatDistanceToNow(date, { addSuffix: true, includeSeconds: true });
	}

	// Convert timestamp to readable date for tooltip
	function formatTimestamp(timestamp: bigint): string {
		return new Date(Number(timestamp) * 1000).toLocaleString();
	}

	// Map priority number to readable string
	function getPriority(priority: number): string {
		return priority === 1 ? 'High' : 'Low';
	}

	// Get color based on priority
	function getPriorityColor(priority: number): string {
		return priority === 1 ? '#ffc107' : '#9b59b6';
	}
</script>

<div
	class="task-card"
	on:mouseenter={() => (isHovering = true)}
	on:mouseleave={() => (isHovering = false)}
>
	<div class="task-header">
		<h3 class="task-name">{task.name} <span class="task-tier">- T{task.tier}</span></h3>
		<span class="priority-badge" style="background-color: {getPriorityColor(task.priority)}">
			{getPriority(task.priority)}
		</span>
	</div>

	<div class="task-details">
		<p class="created-at" title={formatTimestamp(task.createdAt)}>
			Created: {formatRelativeTime(task.createdAt)}
		</p>
		<p
			class="last-transitioned"
			title={task.lastTransitioned ? formatTimestamp(task.lastTransitioned) : 'N/A'}
		>
			Last Transitioned: {task.lastTransitioned ? formatRelativeTime(task.lastTransitioned) : 'N/A'}
		</p>
	</div>

	{#if isCancelable && isHovering}
		<div class="cancel-overlay">
			<button class="cancel-button" on:click|stopPropagation={handleCancel} disabled={cancelling}>
				{cancelling ? 'Cancelling...' : 'Cancel'}
			</button>
		</div>
	{/if}
</div>

<style>
	.task-card {
		background-color: white;
		border-radius: 4px;
		box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
		padding: 0.75rem;
		transition:
			transform 0.2s,
			box-shadow 0.2s;
		position: relative;
	}

	.task-card:hover {
		transform: translateY(-2px);
		box-shadow: 0 3px 6px rgba(0, 0, 0, 0.15);
	}

	.cancel-overlay {
		position: absolute;
		top: 0;
		right: 0;
		bottom: 0;
		left: 0;
		display: flex;
		align-items: center;
		justify-content: center;
		background-color: rgba(0, 0, 0, 0.5);
		border-radius: 4px;
		opacity: 0;
		animation: fade-in 0.2s forwards;
	}

	@keyframes fade-in {
		from {
			opacity: 0;
		}
		to {
			opacity: 1;
		}
	}

	.cancel-button {
		background-color: #ffc107;
		color: white;
		border: none;
		border-radius: 4px;
		padding: 0.5rem 1rem;
		font-size: 0.9rem;
		cursor: pointer;
		font-weight: 500;
		transition: background-color 0.2s;
	}

	.cancel-button:hover:not(:disabled) {
		background-color: #ffb300;
	}

	.cancel-button:disabled {
		background-color: #999;
		cursor: not-allowed;
	}

	.task-header {
		display: flex;
		justify-content: space-between;
		align-items: flex-start;
		margin-bottom: 0.5rem;
	}

	.task-name {
		margin: 0;
		font-size: 1rem;
		font-weight: 500;
		word-break: break-word;
	}

	.task-tier {
		font-size: 0.8rem;
		color: #888;
	}

	.priority-badge {
		font-size: 0.75rem;
		padding: 2px 6px;
		border-radius: 20px;
		color: white;
		font-weight: 500;
		white-space: nowrap;
	}

	.task-details {
		font-size: 0.8rem;
		color: #888;
	}

	.created-at,
	.last-transitioned {
		margin: 0;
	}
</style>
