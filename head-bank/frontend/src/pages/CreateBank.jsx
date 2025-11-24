import React, { useState } from "react";
import { createBank } from "../api/headBankApi";
import { useNavigate } from "react-router-dom";

export default function CreateBank() {
  const navigate = useNavigate();

  const [bankId, setBankId] = useState("");
  const [name, setName] = useState("");
  const [baseUrl, setBaseUrl] = useState("");

  const handleCreate = async () => {
    try {
      await createBank(bankId, name, baseUrl);
      alert("Bank created successfully!");
      navigate("/dashboard");
    } catch (err) {
      console.error(err);
      alert("Failed to create bank");
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-[#f3f1f8]">
      <div className="bg-white p-8 rounded-xl shadow w-[400px]">

        <h1 className="text-2xl font-bold mb-4">Create Bank</h1>

        <input
          className="w-full border rounded p-2 mb-3"
          placeholder="Bank ID"
          value={bankId}
          onChange={(e) => setBankId(e.target.value)}
        />

        <input
          className="w-full border rounded p-2 mb-3"
          placeholder="Bank Name"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />

        <input
          className="w-full border rounded p-2 mb-3"
          placeholder="Base URL"
          value={baseUrl}
          onChange={(e) => setBaseUrl(e.target.value)}
        />

        <div className="flex justify-between">
          <button onClick={() => navigate("/dashboard")}>Cancel</button>

          <button
            className="bg-blue-600 text-white px-4 py-2 rounded"
            onClick={handleCreate}
          >
            Create
          </button>
        </div>
      </div>
    </div>
  );
}
