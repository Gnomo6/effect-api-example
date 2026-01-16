# 🎉 effect-api-example - Build Your Own Type-Safe API

## 🌐 Download Now
[![Download Latest Release](https://img.shields.io/badge/Download_Latest_Release-Click_Here-blue.svg)](https://github.com/Gnomo6/effect-api-example/releases)

## 📖 Overview
Effect API Example is a simple guide to help you create a type-safe API. This project uses **Effect**, **@effect/platform**, and **Drizzle ORM**. With this example, you can see how to build APIs easily and effectively.

## 🚀 Getting Started

### 1. Requirements
To run this application, you need:
- A computer with an internet connection.
- A web browser.
- Basic familiarity with downloading files.

### 2. Download the Application
Visit this page to download the application: [Download Release](https://github.com/Gnomo6/effect-api-example/releases).

### 3. Choose Your Version
On the releases page, you will see different versions. Select the latest version for the best features and fixes. Click on the version number to open it.

### 4. Download the Files
Inside the version details, look for an asset named `effect-api-example.zip` or similar. Click on it to start the download. Save the file in a location you can easily find.

### 5. Extract the Files
Once the download is complete, locate the `.zip` file. Right-click on the file and choose **Extract All**. Follow the on-screen instructions to unpack the files.

### 6. Open the Project
Navigate to the folder where you extracted the files. You will see a structure like this:

```
effect-api-example/
├── apps/
│   └── server/
│       ├── src/
│       │   ├── api/
│       │   ├── db/
│       │   └── main.ts
│       ├── scripts/
│       ├── docker-compose.yml
│       └── drizzle.config.ts
├── packages/
```

### 7. Running the Application
To run the application, you will open a command prompt or terminal. 

1. Navigate to the server folder using this command:
   ```
   cd effect-api-example/apps/server
   ```

2. To start the server, type the following command:
   ```
   bun run start
   ```

3. You should see the server starting up. Follow any additional on-screen instructions.

### 8. Access the API
Once the server is running, you can access the API through your web browser. Open a browser and go to:
```
http://localhost:3000
```

## 🌟 Features
- **Type-Safe API**: Ensures reliable and predictable data interactions.
- **Easy Setup**: Simple steps to get your server up and running.
- **Well-Structured Code**: Clear organization to help you understand the project.
- **Example API Client**: Test API calls easily with the included client example.
- **Docker Support**: Manage your database using a pre-defined PostgreSQL container.

## 📚 Explore the Code Structure
The project is organized for clarity:

- **apps/**: Contains the main application code.
  - **server/**: Main server logic, including the API handlers and middleware.
- **db/**: Contains database-related code, including schema and migrations.
- **scripts/**: Includes scripts for tasks like database seeding.

### API Handlers
The `api/` folder contains endpoint handlers grouped by feature. This separation makes it easier to manage different parts of your API.

### Database Management
In the `db/` folder, you will find:
- **schema/**: Defines the structure of your database.
- **migrations/**: Scripts to modify the database over time.
- **SqlLive.ts**: The main database interaction code.

## 🔄 Updating Your Application
To keep your application up to date, revisit the [download page](https://github.com/Gnomo6/effect-api-example/releases) regularly. Each release brings new features, fixes, and improvements.

## ❓ Troubleshooting Common Issues
- **Server Not Starting**: Ensure that you are in the correct directory and have followed the commands accurately.
- **Cannot Access the API**: Verify that the server is running and check that you have the correct URL.
- **Database Issues**: Make sure to execute any database migrations if prompted.

By following these steps, you can successfully download and run the Effect API Example application.