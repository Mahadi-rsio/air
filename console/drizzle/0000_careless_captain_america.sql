CREATE TABLE `account` (
	`id` text PRIMARY KEY NOT NULL,
	`account_id` text NOT NULL,
	`provider_id` text NOT NULL,
	`user_id` text NOT NULL,
	`access_token` text,
	`refresh_token` text,
	`id_token` text,
	`access_token_expires_at` integer,
	`refresh_token_expires_at` integer,
	`scope` text,
	`password` text,
	`created_at` integer NOT NULL,
	`updated_at` integer NOT NULL,
	FOREIGN KEY (`user_id`) REFERENCES `user`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE TABLE `api_keys` (
	`id` text PRIMARY KEY NOT NULL,
	`workspace_id` text NOT NULL,
	`user_id` text NOT NULL,
	`name` text NOT NULL,
	`prefix` text NOT NULL,
	`key_hash` text NOT NULL,
	`scopes` text DEFAULT '[]' NOT NULL,
	`expires_at` integer,
	`last_used_at` integer,
	`revoked_at` integer,
	`created_at` integer NOT NULL,
	FOREIGN KEY (`workspace_id`) REFERENCES `workspaces`(`id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`user_id`) REFERENCES `user`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE UNIQUE INDEX `api_keys_key_hash_unique` ON `api_keys` (`key_hash`);--> statement-breakpoint
CREATE INDEX `idx_api_keys_workspace` ON `api_keys` (`workspace_id`);--> statement-breakpoint
CREATE TABLE `audit_logs` (
	`id` text PRIMARY KEY NOT NULL,
	`workspace_id` text NOT NULL,
	`project_id` text,
	`user_id` text NOT NULL,
	`action` text NOT NULL,
	`resource_type` text NOT NULL,
	`resource_id` text NOT NULL,
	`metadata` text DEFAULT '{}' NOT NULL,
	`created_at` integer NOT NULL,
	FOREIGN KEY (`workspace_id`) REFERENCES `workspaces`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `idx_audit_logs_workspace` ON `audit_logs` (`workspace_id`);--> statement-breakpoint
CREATE TABLE `branches` (
	`id` text PRIMARY KEY NOT NULL,
	`project_id` text NOT NULL,
	`name` text NOT NULL,
	`parent_branch_id` text,
	`tenant_id` text,
	`status` text DEFAULT 'ACTIVE' NOT NULL,
	`is_default` integer DEFAULT false NOT NULL,
	`created_at` integer NOT NULL,
	`updated_at` integer NOT NULL,
	FOREIGN KEY (`project_id`) REFERENCES `projects`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `idx_branches_project` ON `branches` (`project_id`);--> statement-breakpoint
CREATE TABLE `jwks` (
	`id` text PRIMARY KEY NOT NULL,
	`public_key` text NOT NULL,
	`private_key` text NOT NULL,
	`created_at` integer NOT NULL,
	`expires_at` integer,
	`alg` text,
	`crv` text
);
--> statement-breakpoint
CREATE TABLE `projects` (
	`id` text PRIMARY KEY NOT NULL,
	`workspace_id` text NOT NULL,
	`name` text NOT NULL,
	`description` text DEFAULT '' NOT NULL,
	`tenant_id` text NOT NULL,
	`region` text DEFAULT 'us-east-1' NOT NULL,
	`pg_version` text DEFAULT '17' NOT NULL,
	`compute_size` text DEFAULT '0.25-1cu' NOT NULL,
	`autoscaling_min` text DEFAULT '0.25' NOT NULL,
	`autoscaling_max` text DEFAULT '1' NOT NULL,
	`status` text DEFAULT 'PROVISIONING' NOT NULL,
	`database_name` text DEFAULT 'postgres' NOT NULL,
	`username` text DEFAULT 'app_user' NOT NULL,
	`password_enc` text DEFAULT '' NOT NULL,
	`direct_port` integer DEFAULT 0 NOT NULL,
	`system_identifier` text DEFAULT '' NOT NULL,
	`fencing_epoch` integer DEFAULT 1 NOT NULL,
	`created_by` text NOT NULL,
	`created_at` integer NOT NULL,
	`updated_at` integer NOT NULL,
	FOREIGN KEY (`workspace_id`) REFERENCES `workspaces`(`id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`created_by`) REFERENCES `user`(`id`) ON UPDATE no action ON DELETE no action
);
--> statement-breakpoint
CREATE UNIQUE INDEX `projects_tenant_id_unique` ON `projects` (`tenant_id`);--> statement-breakpoint
CREATE INDEX `idx_projects_workspace` ON `projects` (`workspace_id`);--> statement-breakpoint
CREATE TABLE `query_history` (
	`id` text PRIMARY KEY NOT NULL,
	`project_id` text NOT NULL,
	`branch_id` text,
	`user_id` text NOT NULL,
	`sql` text NOT NULL,
	`duration_ms` integer DEFAULT 0 NOT NULL,
	`row_count` integer DEFAULT 0 NOT NULL,
	`status` text DEFAULT 'success' NOT NULL,
	`error_message` text,
	`created_at` integer NOT NULL,
	FOREIGN KEY (`project_id`) REFERENCES `projects`(`id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`user_id`) REFERENCES `user`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `idx_query_history_project` ON `query_history` (`project_id`);--> statement-breakpoint
CREATE TABLE `saved_queries` (
	`id` text PRIMARY KEY NOT NULL,
	`project_id` text NOT NULL,
	`branch_id` text,
	`user_id` text NOT NULL,
	`name` text NOT NULL,
	`sql` text NOT NULL,
	`created_at` integer NOT NULL,
	`updated_at` integer NOT NULL,
	FOREIGN KEY (`project_id`) REFERENCES `projects`(`id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`user_id`) REFERENCES `user`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE INDEX `idx_saved_queries_project` ON `saved_queries` (`project_id`);--> statement-breakpoint
CREATE TABLE `session` (
	`id` text PRIMARY KEY NOT NULL,
	`expires_at` integer NOT NULL,
	`token` text NOT NULL,
	`created_at` integer NOT NULL,
	`updated_at` integer NOT NULL,
	`ip_address` text,
	`user_agent` text,
	`user_id` text NOT NULL,
	FOREIGN KEY (`user_id`) REFERENCES `user`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE UNIQUE INDEX `session_token_unique` ON `session` (`token`);--> statement-breakpoint
CREATE TABLE `user` (
	`id` text PRIMARY KEY NOT NULL,
	`name` text NOT NULL,
	`email` text NOT NULL,
	`email_verified` integer DEFAULT false NOT NULL,
	`image` text,
	`created_at` integer NOT NULL,
	`updated_at` integer NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX `user_email_unique` ON `user` (`email`);--> statement-breakpoint
CREATE TABLE `verification` (
	`id` text PRIMARY KEY NOT NULL,
	`identifier` text NOT NULL,
	`value` text NOT NULL,
	`expires_at` integer NOT NULL,
	`created_at` integer,
	`updated_at` integer
);
--> statement-breakpoint
CREATE TABLE `workspace_members` (
	`id` text PRIMARY KEY NOT NULL,
	`workspace_id` text NOT NULL,
	`user_id` text NOT NULL,
	`role` text DEFAULT 'member' NOT NULL,
	`created_at` integer NOT NULL,
	FOREIGN KEY (`workspace_id`) REFERENCES `workspaces`(`id`) ON UPDATE no action ON DELETE cascade,
	FOREIGN KEY (`user_id`) REFERENCES `user`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE UNIQUE INDEX `idx_workspace_member_unique` ON `workspace_members` (`workspace_id`,`user_id`);--> statement-breakpoint
CREATE TABLE `workspaces` (
	`id` text PRIMARY KEY NOT NULL,
	`name` text NOT NULL,
	`slug` text NOT NULL,
	`owner_id` text NOT NULL,
	`created_at` integer NOT NULL,
	`updated_at` integer NOT NULL,
	FOREIGN KEY (`owner_id`) REFERENCES `user`(`id`) ON UPDATE no action ON DELETE cascade
);
--> statement-breakpoint
CREATE UNIQUE INDEX `workspaces_slug_unique` ON `workspaces` (`slug`);