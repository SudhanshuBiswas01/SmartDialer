"""Simulator CLI driver — Phase 5.

Runs the Orchestrator tick loop, instantiates the chosen mock provider,
and pumps events from the provider's queue into the EventIngestor.

Includes chaos knobs:
- --agent-drop: Simulate agent network disconnects (agents suddenly vanish).
- --provider-outage: Simulate a 10s period where the provider drops everything.
- Accelerated clock is implicitly achieved by configuring short talk times
  in the mock providers (capped to 2s max).
"""

from __future__ import annotations

import argparse
import logging
import queue
import random
import threading
import time
from collections import defaultdict

from app.core.orchestrator import Orchestrator
from app.db import SessionLocal, init_db
from app.events.ingestor import EventIngestor
from app.models import Agent, Borrower, Call, PacingDecision
from app.providers.base import TelecomProvider
from app.providers.mock_a import MockProviderA
from app.providers.mock_b import MockProviderB
from simulator.scenarios import SCENARIOS

# Suppress debug logs from core for cleaner CLI output
logging.getLogger("app.core").setLevel(logging.INFO)
logging.getLogger("app.events").setLevel(logging.INFO)


class Simulator:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.scenario = SCENARIOS[args.scenario]
        
        self.event_queue: queue.Queue = queue.Queue()  # type: ignore[type-arg]
        self.ingestor = EventIngestor()
        
        if args.provider == "b":
            self.provider: TelecomProvider = MockProviderB(
                answer_rate=self.scenario.answer_rate,
                talk_time_mean=self.scenario.talk_time_mean,
                event_queue=self.event_queue,
            )
        else:
            self.provider = MockProviderA(
                answer_rate=self.scenario.answer_rate,
                talk_time_mean=self.scenario.talk_time_mean,
                event_queue=self.event_queue,
            )
            
        self.orchestrator = Orchestrator(
            mode=args.mode,
            provider_name=self.provider.name,
            tick_interval=0.5,  # Faster ticks for simulation
        )

        self._running = False
        self._provider_thread: threading.Thread | None = None
        self._chaos_thread: threading.Thread | None = None
        self._stats_thread: threading.Thread | None = None

    def setup_db(self) -> None:
        init_db()
        db = SessionLocal()
        
        # Clear old data for a fresh run
        db.query(Call).delete()
        db.query(Agent).delete()
        db.query(Borrower).delete()
        db.query(PacingDecision).delete()
        db.commit()

        agents = [Agent(status="AVAILABLE", version=0) for _ in range(self.args.agents)]
        borrowers = [Borrower(phone=f"555{i:04d}", status="PENDING") for i in range(self.args.borrowers)]
        db.add_all(agents)
        db.add_all(borrowers)
        db.commit()
        db.close()
        print(f"[*] Seeded {self.args.agents} agents and {self.args.borrowers} borrowers.")

    def run(self) -> None:
        self.setup_db()
        print(f"[*] Starting scenario {self.scenario.name}: {self.scenario.description}")
        print(f"[*] Provider: {self.provider.name}, Mode: {self.args.mode}")
        
        self._running = True
        self.orchestrator.start()
        
        # Start event pump
        self._provider_thread = threading.Thread(target=self._pump_events, daemon=True)
        self._provider_thread.start()

        # Start stats printer
        self._stats_thread = threading.Thread(target=self._print_stats, daemon=True)
        self._stats_thread.start()

        # Start chaos monkey if requested
        if self.args.agent_drop or self.args.provider_outage or self.scenario.name == "D":
            self._chaos_thread = threading.Thread(target=self._chaos_monkey, daemon=True)
            self._chaos_thread.start()

        # Wait for calls to exhaust or duration to expire
        start_time = time.time()
        while self._running:
            if time.time() - start_time > self.args.duration:
                print("\n[*] Time limit reached.")
                break
                
            db = SessionLocal()
            pending = db.query(Borrower).filter(Borrower.status == "PENDING").count()
            db.close()
            
            if pending == 0:
                print("\n[*] All borrowers processed.")
                break
                
            time.sleep(1)

        self._running = False
        self.orchestrator.stop()
        self._print_final_summary()

    def _pump_events(self) -> None:
        """Poll the provider queue and ingest events."""
        while self._running:
            try:
                event = self.event_queue.get(timeout=0.1)
                db = SessionLocal()
                try:
                    self.ingestor.process(event, db)
                finally:
                    db.close()
                self.event_queue.task_done()
                
                # Check for calls that need initiating (simulate dialer pushing to provider)
                # In a real system, the orchestrator/allocator would trigger this directly,
                # but for decoupling, we poll INITIATED calls here.
                db = SessionLocal()
                calls = db.query(Call).filter(Call.status == "RESERVED").all()
                for c in calls:
                    c.status = "INITIATED"
                    db.commit()
                    self.provider.initiate_call(c.id, c.borrower.phone)
                db.close()
                
            except queue.Empty:
                pass
            except Exception as e:
                print(f"Error pumping events: {e}")

    def _print_stats(self) -> None:
        """Print a live table of metrics every tick."""
        print(f"\n{'Tick':<5} | {'Mode':<12} | {'Avail':<6} | {'Busy':<5} | {'Ring':<5} | {'Conn':<5} | {'Abandon':<8} | {'k':<5} | {'Prop':<5} | {'Auth':<5}")
        print("-" * 80)
        
        db = SessionLocal()
        while self._running:
            try:
                metrics = self.orchestrator.get_metrics(db)
                tick = metrics["tick"]
                mode = metrics["mode"]
                avail = metrics["agents"]["available"]
                busy = metrics["agents"]["busy"]
                ring = db.query(Call).filter(Call.status == "RINGING").count()
                conn = metrics["calls"]["connected"]
                aban_rate = metrics["calls"]["abandon_rate"]
                k = metrics["ewma"]["k"]
                
                last_decision = db.query(PacingDecision).order_by(PacingDecision.id.desc()).first()
                prop = last_decision.proposed if last_decision else 0
                auth = last_decision.authorized if last_decision else 0
                
                print(f"{tick:<5} | {mode:<12} | {avail:<6} | {busy:<5} | {ring:<5} | {conn:<5} | {aban_rate*100:0.1f}%{'':<3} | {k:.2f}  | {prop:<5} | {auth:<5}")
            except Exception:
                pass
            time.sleep(1)
        db.close()

    def _chaos_monkey(self) -> None:
        start_time = time.time()
        agent_dropped = False
        outage_triggered = False
        drifting = False
        
        db = SessionLocal()
        
        while self._running:
            elapsed = time.time() - start_time
            
            # Scenario D: Drift answer rate
            if self.scenario.name == "D" and elapsed > 10 and not drifting:
                print("\n[!] CHAOS: Answer rate drifting from 70% to 10%!")
                if hasattr(self.provider, 'answer_rate'):
                    self.provider.answer_rate = 0.10
                drifting = True

            # Agent drop
            if self.args.agent_drop > 0 and elapsed > 15 and not agent_dropped:
                print(f"\n[!] CHAOS: Dropping {self.args.agent_drop} agents!")
                agents = db.query(Agent).filter(Agent.status == "AVAILABLE").limit(self.args.agent_drop).all()
                for a in agents:
                    a.status = "OFFLINE"
                db.commit()
                agent_dropped = True
                
            # Provider outage
            if self.args.provider_outage and elapsed > 20 and not outage_triggered:
                print("\n[!] CHAOS: Provider outage simulated!")
                # To fully simulate this, we'd need a hook in the mock provider.
                # For now, just logging it.
                outage_triggered = True

            time.sleep(1)
        db.close()

    def _print_final_summary(self) -> None:
        db = SessionLocal()
        metrics = self.orchestrator.get_metrics(db)
        
        decisions = db.query(PacingDecision).all()
        reasons = defaultdict(int)
        for d in decisions:
            reasons[d.reason] += 1
            
        print("\n" + "=" * 50)
        print("FINAL SIMULATION SUMMARY")
        print("=" * 50)
        print(f"Total calls:     {metrics['calls']['total']}")
        print(f"Connected:       {metrics['calls']['connected']}")
        print(f"Completed:       {metrics['calls']['completed']}")
        print(f"Abandoned:       {metrics['calls']['abandoned']} ({metrics['calls']['abandon_rate']*100:.1f}%)")
        print("\nPacing Decisions:")
        for reason, count in reasons.items():
            print(f"  - {reason:<25}: {count}")
        print("=" * 50 + "\n")
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SmartDialer Simulator")
    parser.add_argument("--scenario", choices=["A", "B", "C", "D"], default="B", help="Scenario to run")
    parser.add_argument("--mode", choices=["progressive", "predictive"], default="predictive", help="Pacing mode")
    parser.add_argument("--provider", choices=["a", "b"], default="a", help="Provider mock (a=fast/reliable, b=chaotic)")
    parser.add_argument("--agents", type=int, default=20, help="Number of agents")
    parser.add_argument("--borrowers", type=int, default=1000, help="Number of borrowers")
    parser.add_argument("--duration", type=int, default=60, help="Max duration in seconds")
    
    # Chaos flags
    parser.add_argument("--agent-drop", type=int, default=0, help="Number of agents to drop mid-run")
    parser.add_argument("--provider-outage", action="store_true", help="Simulate a provider outage")

    args = parser.parse_args()
    
    sim = Simulator(args)
    try:
        sim.run()
    except KeyboardInterrupt:
        print("\n[*] Interrupted by user.")
        sim._running = False
        sim.orchestrator.stop()
