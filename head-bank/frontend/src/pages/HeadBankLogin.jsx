// src/pages/HeadBankLogin.jsx
import React from "react";
import { useNavigate } from "react-router-dom";

export default function HeadBankLogin() {
  const navigate = useNavigate();

  return (
    <div className="flex flex-col gap-4 p-10 max-w-sm mx-auto mt-20 bg-white shadow rounded-xl">
      <h1 className="text-3xl font-bold">Head Bank Login</h1>

      <input
        type="text"
        className="border p-3 rounded"
        placeholder="Admin ID"
      />

      <input
        type="password"
        className="border p-3 rounded"
        placeholder="Password"
      />

      <button
        onClick={() => navigate("/dashboard")}
        className="bg-blue-600 text-white p-3 rounded text-center"
      >
        Login
      </button>

      {/* OPTIONAL SWITCH */}
      <button
        className="underline text-sm text-gray-500"
        onClick={() => (window.location.href = "http://localhost:3002")}
      >
        Switch to Teller →
      </button>
    </div>
  );
}
