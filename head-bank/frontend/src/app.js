import { BrowserRouter, Routes, Route } from "react-router-dom";
import HeadBankLogin from "./pages/HeadBankLogin";
import HeadBankDashboard from "./pages/HeadBankDashboard";
import CreateBank from "./pages/CreateBank";
import DeleteBank from "./pages/DeleteBank";

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<HeadBankLogin />} />
        <Route path="/dashboard" element={<HeadBankDashboard />} />
        <Route path="/create-bank" element={<CreateBank />} />
        <Route path="/delete-bank" element={<DeleteBank />} />
      </Routes>
    </BrowserRouter>
  );
}

export default App;
