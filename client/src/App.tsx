import { Toaster } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import NotFound from "@/pages/NotFound";
import { Route, Switch } from "wouter";
import ErrorBoundary from "./components/ErrorBoundary";
import { ThemeProvider } from "./contexts/ThemeContext";
import Dashboard from "./pages/Dashboard";
import AutonomousMonitor from "./pages/AutonomousMonitor";
import RealTimeTrading from './pages/RealTimeTrading';
import UnifiedTradingSystem from './pages/UnifiedTradingSystem';
import PremiumDashboard from './pages/PremiumDashboard';
import SofiaAIDashboard from './pages/SofiaAIDashboard';
import SofiaDashboardDB from './pages/SofiaDashboardDB';
import RealDashboard from './pages/RealDashboard';
import MLPerformance from './pages/MLPerformance';
import DerivativesDashboard from './pages/DerivativesDashboard';

function Router() {
  // make sure to consider if you need authentication for certain routes
  return (
    <Switch>
      <Route path="/derivatives" component={DerivativesDashboard} />
      <Route path="/ml-performance" component={MLPerformance} />
      <Route path="/sofia-db" component={SofiaDashboardDB} />
      <Route path="/sofia" component={SofiaAIDashboard} />
      <Route path="/unified" component={UnifiedTradingSystem} />
      <Route path="/trading" component={RealTimeTrading} />
      <Route path={"/autonomous"} component={AutonomousMonitor} />
      <Route path={"/"} component={RealDashboard} />
      <Route path={"/premium"} component={PremiumDashboard} />
      <Route path={"/404"} component={NotFound} />
      {/* Final fallback route */}
      <Route component={NotFound} />
    </Switch>
  );
}

// NOTE: About Theme
// - First choose a default theme according to your design style (dark or light bg), than change color palette in index.css
//   to keep consistent foreground/background color across components
// - If you want to make theme switchable, pass `switchable` ThemeProvider and use `useTheme` hook

function App() {
  return (
    <ErrorBoundary>
      <ThemeProvider
        defaultTheme="dark"
        // switchable
      >
        <TooltipProvider>
          <Toaster />
          <Router />
        </TooltipProvider>
      </ThemeProvider>
    </ErrorBoundary>
  );
}

export default App;
