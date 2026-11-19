import axios from "axios";

export const apiClient = axios.create({
  baseURL: "/api",
  timeout: 30000,
  headers: {
    "Content-Type": "application/json",
  },
  withCredentials: true,
});

apiClient.interceptors.response.use(
  (response) => response,
  (error) => {
    const message =
      error.response?.data?.error ||
      error.message ||
      "An unexpected request error occurred";
    return Promise.reject(new Error(message));
  }
);
