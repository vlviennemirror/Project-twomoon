import axios from "axios";

const api = axios.create({
  baseURL: "",
  withCredentials: true,
  headers: {
    "Content-Type": "application/json",
  },
  timeout: 15000,
});

api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      const isAuthMe = error.config?.url?.includes("/auth/me");
      if (!isAuthMe) {
        window.location.href = "/login";
      }
    }
    return Promise.reject(error);
  }
);

export default api;
