import axios from "axios";

const api = axios.create({
  baseURL: process.env.REACT_APP_HEADBANK_URL,
});

// Add token support later (not needed now)

export const getBanks = () => api.get("/banks");

export const createBank = (bankId, name, baseUrl) =>
  api.post("/banks", {
    bank_id: bankId,
    name,
    base_url: baseUrl,
  });

export const deleteBank = (bankId) =>
  api.delete(`/banks/${bankId}`);

export const searchBank = (bankId) =>
  api.get(`/banks/${bankId}`);

export default api;
