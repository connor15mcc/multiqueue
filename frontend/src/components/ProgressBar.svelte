<script lang="ts">
	// Props for the progress bar
	export let totalCount: number;
	export let completeCount: number;
	export let cancelledCount: number;
	export let tierStats: Array<{
		tier: number;
		totalCount: number;
		completeCount: number;
		cancelledCount: number;
	}> = [];

	// Calculate percentages for main progress bar
	$: percentageComplete = totalCount > 0 ? (completeCount / totalCount) * 100 : 0;
	$: percentageCancelled = totalCount > 0 ? (cancelledCount / totalCount) * 100 : 0;
	$: percentageInProgress = 100 - percentageComplete - percentageCancelled;

	// Format percentages for display
	$: displayComplete = `${Math.round(percentageComplete)}%`;
	$: displayInProgress = `${Math.round(percentageInProgress)}%`;
	$: displayCancelled = `${Math.round(percentageCancelled)}%`;

	// Calculate percentages for tier progress bars
	$: tierPercentages = tierStats.map((tier) => {
		const total = tier.totalCount;
		return {
			tier: tier.tier,
			complete: total > 0 ? (tier.completeCount / total) * 100 : 0,
			cancelled: total > 0 ? (tier.cancelledCount / total) * 100 : 0,
			inProgress:
				total > 0
					? 100 - (tier.completeCount / total) * 100 - (tier.cancelledCount / total) * 100
					: 0,
			totalCount: total
		};
	});

	// Helper function to get tier name
	function getTierName(tier: number): string {
		return `Tier ${tier}`;
	}
</script>

<div class="progress-container">
	<div class="main-progress-label">Overall Progress</div>
	<div class="progress-bar">
		<div class="progress-segment complete" style="width: {percentageComplete}%">
			{#if percentageComplete > 10}
				<span class="segment-text">{displayComplete}</span>
			{/if}
		</div>
		<div class="progress-segment in-progress" style="width: {percentageInProgress}%">
			{#if percentageInProgress > 10}
				<span class="segment-text">{displayInProgress}</span>
			{/if}
		</div>
		<div class="progress-segment cancelled" style="width: {percentageCancelled}%">
			{#if percentageCancelled > 10}
				<span class="segment-text">{displayCancelled}</span>
			{/if}
		</div>
	</div>

	<div class="progress-legend">
		<div class="legend-item">
			<div class="legend-color complete"></div>
			<span class="legend-label">Complete: {displayComplete}</span>
		</div>
		<div class="legend-item">
			<div class="legend-color in-progress"></div>
			<span class="legend-label">In Progress: {displayInProgress}</span>
		</div>
		<div class="legend-item">
			<div class="legend-color cancelled"></div>
			<span class="legend-label">Cancelled: {displayCancelled}</span>
		</div>
	</div>

	<!-- Tier-specific progress bars -->
	<div class="tier-progress-section">
		<div class="tier-grid">
			{#each tierPercentages as tierData}
				<div class="tier-progress-container">
					<div class="tier-label">{getTierName(tierData.tier)} ({tierData.totalCount})</div>
					<div class="progress-bar tier-bar">
						<div class="progress-segment complete" style="width: {tierData.complete}%">
							{#if tierData.complete > 10}
								<span class="segment-text-small">{Math.round(tierData.complete)}%</span>
							{/if}
						</div>
						<div class="progress-segment in-progress" style="width: {tierData.inProgress}%">
							{#if tierData.inProgress > 10}
								<span class="segment-text-small">{Math.round(tierData.inProgress)}%</span>
							{/if}
						</div>
						<div class="progress-segment cancelled" style="width: {tierData.cancelled}%">
							{#if tierData.cancelled > 10}
								<span class="segment-text-small">{Math.round(tierData.cancelled)}%</span>
							{/if}
						</div>
					</div>
				</div>
			{/each}
		</div>
	</div>
</div>

<style>
	.progress-container {
		width: 100%;
		margin: 1rem 0;
		padding: 0 1rem;
	}

	.main-progress-label {
		margin-bottom: 0.5rem;
		font-weight: 500;
		font-size: 1.1rem;
	}

	.progress-bar {
		height: 20px;
		border-radius: 10px;
		overflow: hidden;
		display: flex;
		box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
	}

	.progress-segment {
		height: 100%;
		display: flex;
		align-items: center;
		justify-content: center;
		transition: width 0.5s ease;
		position: relative;
	}

	.segment-text {
		color: white;
		font-size: 0.8rem;
		font-weight: bold;
		text-shadow: 0 0 2px rgba(0, 0, 0, 0.5);
	}

	.complete {
		background-color: #d6f651;
	}

	.in-progress {
		background-color: #9b59b6;
	}

	.cancelled {
		background-color: #ffc107;
	}

	.progress-legend {
		display: flex;
		justify-content: center;
		gap: 1rem;
		margin-top: 0.5rem;
		flex-wrap: wrap;
		margin-bottom: 0.75rem;
	}

	.legend-item {
		display: flex;
		align-items: center;
		font-size: 0.8rem;
	}

	.legend-color {
		width: 12px;
		height: 12px;
		border-radius: 2px;
		margin-right: 4px;
	}

	.legend-label {
		color: #666;
	}

	.tier-progress-section {
		margin-top: 1rem;
	}

	.tier-grid {
		display: grid;
		grid-template-columns: repeat(4, 1fr);
		gap: 1rem;
	}

	.tier-progress-container {
		margin-bottom: 0.5rem;
	}

	.tier-label {
		font-size: 0.8rem;
		margin-bottom: 0.25rem;
		display: flex;
		justify-content: space-between;
		align-items: center;
		color: #555;
	}

	.tier-bar {
		height: 8px;
	}

	.segment-text-small {
		color: white;
		font-size: 0.7rem;
		font-weight: bold;
		text-shadow: 0 0 1px rgba(0, 0, 0, 0.5);
	}

	@media (max-width: 768px) {
		.tier-grid {
			grid-template-columns: repeat(2, 1fr);
		}
	}

	@media (max-width: 480px) {
		.tier-grid {
			grid-template-columns: 1fr;
		}
	}
</style>
