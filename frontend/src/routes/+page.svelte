<script lang="ts">
	import TaskList from '../components/TaskList.svelte';
	import TaskForm from '../components/TaskForm.svelte';
	import ProgressBar from '../components/ProgressBar.svelte';
	import { submitTask, fetchStats } from '../lib/api';
	import { onDestroy, onMount } from 'svelte';

	let taskStates = [
		{ id: 3, name: 'Cancelled', count: 0 },
		{ id: 0, name: 'Waiting', count: 0 },
		{ id: 1, name: 'Queued', count: 0 },
		{ id: 2, name: 'Complete', count: 0 }
	];

	// Progress bar stats
	let totalCount = 0;
	let completeCount = 0;
	let cancelledCount = 0;

	// Tier-specific stats
	let tierStats = [
		{ tier: 0, totalCount: 0, completeCount: 0, cancelledCount: 0 },
		{ tier: 1, totalCount: 0, completeCount: 0, cancelledCount: 0 },
		{ tier: 2, totalCount: 0, completeCount: 0, cancelledCount: 0 },
		{ tier: 3, totalCount: 0, completeCount: 0, cancelledCount: 0 }
	];

	let generatingTasks = false;
	let generationResult = '';
	let loading = true;

	// Set up stats fetching
	async function refreshStats() {
		try {
			// Fetch overall stats
			const stats = await fetchStats();

			if (stats) {
				// Update task counts
				taskStates[0].count = Number(stats.cancelledCount);
				taskStates[1].count = Number(stats.waitingCount);
				taskStates[2].count = Number(stats.queuedCount);
				taskStates[3].count = Number(stats.completeCount);

				// Update progress bar stats
				totalCount = Number(stats.totalCount);
				completeCount = Number(stats.completeCount);
				cancelledCount = Number(stats.cancelledCount);
			}

			// Fetch tier-specific stats
			const tierPromises = [0, 1, 2, 3].map(async (tier) => {
				const tierStat = await fetchStats(tier);
				if (tierStat) {
					tierStats[tier] = {
						tier,
						totalCount: Number(tierStat.totalCount),
						completeCount: Number(tierStat.completeCount),
						cancelledCount: Number(tierStat.cancelledCount)
					};
				}
			});

			// Wait for all tier stats to be fetched
			await Promise.all(tierPromises);

			loading = false;
		} catch (error) {
			console.error('Failed to fetch stats:', error);
			loading = false;
		}
	}

	// Set up polling for stats updates
	const STATS_REFRESH_INTERVAL = 1000; // 1 second
	let statsInterval: ReturnType<typeof setInterval>;

	onMount(() => {
		refreshStats();
		statsInterval = setInterval(refreshStats, STATS_REFRESH_INTERVAL);
	});

	onDestroy(() => {
		if (statsInterval) {
			clearInterval(statsInterval);
		}
	});

	async function generateAlphabetTasks() {
		if (generatingTasks) return;

		generatingTasks = true;
		generationResult = 'Generating 26 tasks...';

		const taskNames = [
			'alpha',
			'bravo',
			'charlie',
			'delta',
			'echo',
			'foxtrot',
			'golf',
			'hotel',
			'india',
			'juliet',
			'kilo',
			'lima',
			'mike',
			'november',
			'oscar',
			'papa',
			'quebec',
			'romeo',
			'sierra',
			'tango',
			'uniform',
			'victor',
			'whiskey',
			'xray',
			'yankee',
			'zulu'
		];

		try {
			let successCount = 0;

			for (const name of taskNames) {
				try {
					const priority = Math.random() > 0.75 ? 1 : 0;
					// const priority = 0;
					const tier = () => {
						const r = Math.random();
						if (r < 0.4) {
							return 3;
						}
						if (r < 0.67) {
							return 2;
						}
						if (r < 0.87) {
							return 1;
						}
						return 0;
					};
					await submitTask(name, priority, tier());
					successCount++;
				} catch (err) {
					console.error(`Failed to submit task ${name}:`, err);
				}
			}

			generationResult = `Successfully generated ${successCount} of 26 tasks`;
			setTimeout(() => {
				generationResult = '';
			}, 5000);
		} catch (error) {
			console.error('Error generating tasks:', error);
			generationResult = 'Error generating tasks';
			setTimeout(() => {
				generationResult = '';
			}, 5000);
		} finally {
			generatingTasks = false;
		}
	}
</script>

<div class="header-container">
	<h1>MultiQueue Task Monitor</h1>
</div>

<ProgressBar {totalCount} {completeCount} {cancelledCount} {tierStats} />

<div class="form-button-container">
	<div class="form-container">
		<TaskForm />
	</div>
	<div class="button-wrapper">
		<button class="generate-button" on:click={generateAlphabetTasks} disabled={generatingTasks}>
			{generatingTasks ? 'Generating...' : 'Generate A-Z Tasks'}
		</button>
		{#if generationResult}
			<div class="generation-result">{generationResult}</div>
		{/if}
	</div>
</div>

<div class="task-container">
	<!-- Put Cancelled list first -->
	{#each taskStates.filter((state) => state.name === 'Cancelled') as state}
		<TaskList
			stateName={state.name}
			stateId={state.id}
			count={state.count}
			collapsible={true}
			initiallyCollapsed={true}
		/>
	{/each}

	<!-- Then show the remaining task lists -->
	{#each taskStates.filter((state) => state.name !== 'Cancelled') as state}
		<TaskList stateName={state.name} stateId={state.id} count={state.count} />
	{/each}
</div>

<style>
	.header-container {
		display: flex;
		align-items: center;
		margin-bottom: 1rem;
		padding: 0 1rem;
	}

	.form-button-container {
		display: flex;
		align-items: flex-start;
		gap: 1rem;
		padding: 0 1rem;
		margin-bottom: 1rem;
	}

	.form-container {
		flex: 1;
	}

	.button-wrapper {
		display: flex;
		flex-direction: column;
		align-items: stretch;
		width: 180px;
		height: 100%;
	}

	h1 {
		margin: 0;
		text-align: left;
	}

	.button-container {
		display: flex;
		flex-direction: column;
		align-items: flex-end;
		gap: 0.25rem;
	}

	.buttons {
		display: flex;
		gap: 0.5rem;
	}

	.generate-button {
		padding: 0.5rem 1rem;
		color: white;
		border: none;
		border-radius: 4px;
		cursor: pointer;
		font-size: 1rem;
		white-space: nowrap;
		transition: background-color 0.2s;
		background-color: #8e44ad;
		height: 100%;
		width: 100%;
		display: flex;
		align-items: center;
		justify-content: center;
	}

	.generate-button:hover:not(:disabled) {
		background-color: #6c3483;
	}

	.generate-button:disabled {
		background-color: #cccccc;
		cursor: not-allowed;
	}

	.generation-result {
		font-size: 0.8rem;
		text-align: center;
		margin-top: 0.25rem;
		color: #2196f3;
	}

	.task-container {
		display: flex;
		flex-wrap: wrap;
		gap: 1rem;
		margin: 0 auto;
		padding: 1rem;
	}

	@media (min-width: 1200px) {
		.task-container {
			flex-wrap: nowrap;
		}
	}

	@media (min-width: 1024px) {
		.task-container {
			grid-template-columns: repeat(4, 1fr);
		}
	}

	@media (max-width: 600px) {
		.header-container {
			flex-direction: column;
			gap: 1rem;
		}

		.button-container {
			align-items: center;
		}
	}
</style>
