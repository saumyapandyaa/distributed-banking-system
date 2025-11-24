import React, { useState } from "react";
import { deleteBank } from "../api/headBankApi";
import { useNavigate } from "react-router-dom";

export default function DeleteBank() {
  const navigate = useNavigate();
  const [bankId, setBankId] = useState("");

  const handleDelete = async () => {
    try {
      await deleteBank(bankId);
      alert("Bank deleted successfully!");
      navigate("/dashboard");
    } catch {
      alert("Failed to delete bank");
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center bg-[#f3f1f8]">
      <div className="bg-white p-8 rounded-xl shadow w-[400px]">

        <h1 className="text-2xl font-bold mb-4">Delete Bank</h1>

        <input
          className="w-full border rounded p-2 mb-4"
          placeholder="Bank ID"
          value={bankId}
          onChange={(e) => setBankId(e.target.value)}
        />

        <div className="flex justify-between">
          <button onClick={() => navigate("/dashboard")}>Cancel</button>

          <button
            className="bg-red-600 text-white px-4 py-2 rounded"
            onClick={handleDelete}
          >
            Delete
          </button>
        </div>

      </div>
    </div>
  );
}
