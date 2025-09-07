<script lang="ts">
	import { submitTask } from '../lib/api';
	import { createEventDispatcher } from 'svelte';

	const dispatch = createEventDispatcher();

	let taskName = '';
	let priority = 0;
	let tier = 3; // Default tier is 3
	let submitting = false;
	let error = '';
	let successMessage = '';
	let nameInputElement: HTMLInputElement;

	async function handleSubmit() {
		if (!taskName.trim()) {
			error = 'Task name cannot be empty';
			return;
		}

		try {
			submitting = true;
			error = '';
			successMessage = '';

			const result = await submitTask(taskName, priority, tier);

			if (result) {
				dispatch('taskSubmit', { task: result });
				taskName = '';
				priority = 0;
				tier = 3;
				successMessage = `Task "${result.name}" submitted successfully!`;

				// Focus back on the input field
				setTimeout(() => {
					nameInputElement.focus();
				}, 0);

				setTimeout(() => {
					successMessage = '';
				}, 3000);
			} else {
				error = 'Failed to submit task';
			}
		} catch (err) {
			console.error('Failed to submit task:', err);
			error = err instanceof Error ? err.message : 'Unknown error';
		} finally {
			submitting = false;
		}
	}
</script>

<div class="task-form-container">
	<h2>Submit New Task</h2>

	<form on:submit|preventDefault={handleSubmit} class="task-form">
		<div class="form-row">
			<div class="input-container">
				<input
					type="text"
					id="taskName"
					bind:value={taskName}
					bind:this={nameInputElement}
					disabled={submitting}
					placeholder="Enter task name"
					data-sveltekit-keepfocus
				/>
			</div>

			<div class="input-container">
				<input
					type="number"
					id="taskTier"
					bind:value={tier}
					disabled={submitting}
					placeholder="3"
					min="0"
					max="3"
				/>
			</div>

			<div class="radio-group">
				<label for="priority-low" class="priority-label">
					<input
						id="priority-low"
						type="radio"
						name="priority"
						value={0}
						bind:group={priority}
						disabled={submitting}
					/>
					Low
				</label>
				<label for="priority-high" class="priority-label">
					<input
						id="priority-high"
						type="radio"
						name="priority"
						value={1}
						bind:group={priority}
						disabled={submitting}
					/>
					High
				</label>
			</div>

			<button type="submit" class="submit-button" disabled={submitting}>
				{submitting ? 'Submitting...' : 'Submit'}
			</button>
		</div>

		{#if error}
			<div class="error-message">{error}</div>
		{/if}

		{#if successMessage}
			<div class="success-message">{successMessage}</div>
		{/if}
	</form>
</div>

<style>
	.task-form-container {
		background-color: #f5f5f5;
		border-radius: 8px;
		box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
		padding: 0.75rem 1rem;
		margin-bottom: 1rem;
		width: 100%;
	}

	h2 {
		margin-top: 0;
		margin-bottom: 0.75rem;
		font-size: 1.1rem;
		color: #333;
		border-bottom: 1px solid #ddd;
		padding-bottom: 0.5rem;
	}

	.task-form {
		display: flex;
		flex-direction: column;
		gap: 0.5rem;
	}

	.form-row {
		display: flex;
		align-items: center;
		gap: 0.75rem;
		width: 100%;
	}

	.input-container {
		flex: 1;
		display: flex;
		justify-content: end;
	}

	input[type='text'] {
		padding: 0.5rem;
		border: 1px solid #ddd;
		border-radius: 4px;
		font-size: 1rem;
		width: 100%;
	}

	input[type='number'] {
		padding: 0.5rem;
		border: 1px solid #ddd;
		border-radius: 4px;
		font-size: 1rem;
	}

	.radio-group {
		display: flex;
		gap: 1rem;
		white-space: nowrap;
	}

	.priority-label {
		display: flex;
		align-items: center;
		gap: 0.25rem;
		cursor: pointer;
		font-size: 0.9rem;
		color: #555;
	}

	.error-message {
		color: #e53935;
		font-size: 0.9rem;
		padding: 0.25rem 0;
	}

	.success-message {
		color: #43a047;
		font-size: 0.9rem;
		padding: 0.25rem 0;
	}

	.submit-button {
		padding: 0.5rem 1rem;
		background-color: #4caf50;
		color: white;
		border: none;
		border-radius: 4px;
		cursor: pointer;
		font-size: 0.9rem;
		white-space: nowrap;
		min-width: 80px;
	}

	.submit-button:hover:not(:disabled) {
		background-color: #45a049;
	}

	.submit-button:disabled {
		background-color: #cccccc;
		cursor: not-allowed;
	}
</style>
