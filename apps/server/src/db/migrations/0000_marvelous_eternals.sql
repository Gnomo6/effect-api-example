CREATE TYPE "public"."employee_type" AS ENUM('full_time', 'hourly');--> statement-breakpoint
CREATE TABLE "employees" (
	"id" integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY (sequence name "employees_id_seq" INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START WITH 1 CACHE 1),
	"tag" varchar NOT NULL,
	"position" varchar NOT NULL,
	"type" "employee_type" DEFAULT 'full_time' NOT NULL,
	"email" varchar NOT NULL,
	"first_name" varchar NOT NULL,
	"last_name" varchar NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL,
	CONSTRAINT "employees_tag_unique" UNIQUE("tag"),
	CONSTRAINT "employees_email_unique" UNIQUE("email")
);
