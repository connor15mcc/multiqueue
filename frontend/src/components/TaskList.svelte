<script lang="ts">
	import TaskCard from './TaskCard.svelte';
	import { onDestroy, onMount } from 'svelte';
	import { fetchTasks as fetchTasksAPI } from '../lib/api';
	import { type Task, TaskState } from '../lib/proto/multiqueue_pb';

	export let stateName: string;
	export let stateId: number;
	export let count: number;
	export let collapsible: boolean = false;
	export let initiallyCollapsed: boolean = false;

	// Default number of tasks to show
	const defaultVisibleTasks = 5;

	let loading = true;
	let tasks: Task[] = [];
	let expanded = false;
	let collapsed = initiallyCollapsed;

	let promise = async () => {
		tasks = await fetchTasksAPI(stateId as TaskState, 20);
		loading = false;
	};

	const interval = setInterval(async () => {
		promise();
	}, 500);
	onMount(async () => {
		promise();
	});
	onDestroy(() => clearInterval(interval));

	// Function to toggle expanded state
	function toggleExpand() {
		expanded = !expanded;
	}

	// Function to toggle collapsed state
	function toggleCollapse() {
		collapsed = !collapsed;
	}

	// Computed property for visible tasks based on expanded state
	$: visibleTasks = expanded ? tasks : tasks.slice(0, defaultVisibleTasks);
</script>

<div
	class="task-list"
	class:collapsible
	class:collapsed={collapsible && collapsed}
	on:taskSubmit={promise}
>
	<h2 on:click={collapsible ? toggleCollapse : null} class:clickable={collapsible}>
		<span class="title-wrapper">
			{#if collapsible}
				<span class="collapse-icon">{collapsed ? '▶' : '▼'}</span>
			{/if}
			{stateName} Tasks
		</span>
		<span class="task-count">{count}</span>
	</h2>

	{#if !(collapsible && collapsed)}
		{#if loading}
			<div class="loading">Loading tasks...</div>
		{:else if tasks.length === 0}
			<div class="empty">No tasks in {stateName.toLowerCase()} state</div>
		{:else}
			<div class="tasks">
				{#each visibleTasks as task}
					<TaskCard {task} on:taskCancelled={() => promise()} />
				{/each}
			</div>

			{#if tasks.length > defaultVisibleTasks}
				<button class="expand-button" on:click={toggleExpand}>
					{expanded ? 'Show Less' : `Show All (${tasks.length})`}
				</button>
			{/if}
		{/if}
	{/if}
</div>

<style>
	.task-list {
		background-color: #f5f5f5;
		border-radius: 8px;
		box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
		padding: 1rem;
		display: flex;
		flex-direction: column;
		transition: all 0.3s ease;
		flex: 1;
		min-width: 300px;
		min-height: 300px;
		height: auto;
	}

	.task-list.collapsible {
		transition: all 0.3s ease;
	}

	.task-list.collapsed {
		min-width: 40px;
		max-width: 40px;
		flex: 0;
		padding: 0.5rem;
		background-color: #f0f0f0;
		overflow: hidden;
		writing-mode: vertical-lr;
		transform: rotate(180deg);
		min-height: 200px;
		max-height: 300px;
		height: auto;
	}

	.task-list.collapsed h2 {
		margin: 0;
		padding: 0;
		border-bottom: none;
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 0.5rem;
		white-space: nowrap;
		width: 100%;
		height: 100%;
	}

	.task-list.collapsed .title-wrapper {
		display: flex;
		gap: 0.25rem;
		align-items: center;
	}

	.task-list.collapsed .task-count {
		margin-left: 0;
	}

	h2 {
		margin-top: 0;
		margin-bottom: 1rem;
		font-size: 1.2rem;
		color: #333;
		border-bottom: 1px solid #ddd;
		padding-bottom: 0.5rem;
		display: flex;
		justify-content: space-between;
		align-items: center;
		white-space: nowrap;
	}

	.clickable {
		cursor: pointer;
	}

	.clickable:hover {
		color: #1976d2;
	}

	.title-wrapper {
		display: flex;
		align-items: center;
		gap: 0.5rem;
	}

	.collapse-icon {
		font-size: 0.8rem;
		color: #666;
	}

	.task-count {
		font-size: 0.9rem;
		background-color: #e0e0e0;
		color: #333;
		border-radius: 12px;
		padding: 2px 8px;
		min-width: 24px;
		text-align: center;
		font-weight: normal;
	}

	.tasks {
		display: flex;
		flex-direction: column;
		gap: 0.75rem;
		overflow-y: auto;
		flex: 1;
	}

	.loading,
	.empty {
		display: flex;
		align-items: center;
		justify-content: center;
		height: 100px;
		color: #666;
		font-style: italic;
		flex: 1;
	}

	.expand-button {
		margin-top: 0.5rem;
		padding: 0.25rem 0.5rem;
		background-color: transparent;
		color: #666;
		border: 1px solid #ccc;
		border-radius: 4px;
		cursor: pointer;
		font-size: 0.8rem;
		align-self: center;
		transition: all 0.2s ease;
	}

	.expand-button:hover {
		background-color: #f0f0f0;
		color: #333;
	}
</style>
