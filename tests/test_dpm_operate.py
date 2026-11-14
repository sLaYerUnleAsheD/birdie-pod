import unittest
from pathlib import Path
from dpm.discover import build_waves
from dpm.design import assign_medallion_layers, generate_data_contract
from dpm.operate import generate_workflow_dag, check_approval_gate, ApprovalGateError


class TestDpmOperate(unittest.TestCase):
    def tearDown(self):
        p = Path("tests/test_approvals.json")
        if p.exists():
            try:
                p.unlink()
            except Exception:
                pass
    def test_waves_toposort(self):
        jobs = [
            {"id": "j1", "inputs": ["raw_claims"], "outputs": ["stg_claims"]},
            {"id": "j2", "inputs": ["stg_claims"], "outputs": ["mart_claims"]},
        ]
        waves = build_waves(jobs)
        self.assertEqual(len(waves), 2)
        self.assertEqual(waves[0], ["j1"])
        self.assertEqual(waves[1], ["j2"])

    def test_medallion_layer_assignment(self):
        jobs = [
            {"id": "j1", "inputs": ["raw_claims"], "outputs": ["stg_claims"]},
            {"id": "j2", "inputs": ["stg_claims"], "outputs": ["mart_claims"]},
        ]
        layers = assign_medallion_layers(jobs)
        self.assertEqual(layers["raw_claims"], "bronze")
        self.assertEqual(layers["stg_claims"], "silver")
        self.assertEqual(layers["mart_claims"], "gold")

    def test_workflow_dag_generation(self):
        jobs = [
            {"id": "j1", "tier_code": "A", "inputs": ["raw"], "outputs": ["stg"]},
            {"id": "j2", "tier_code": "B", "inputs": ["stg"], "outputs": ["gold"]},
        ]
        dag = generate_workflow_dag(jobs)
        self.assertIn("tasks", dag)
        self.assertEqual(len(dag["tasks"]), 2)
        self.assertEqual(dag["tasks"][1]["depends_on"], ["j1"])

    def test_approval_gate_blocks_unapproved(self):
        # Should raise ApprovalGateError when job is not approved
        with self.assertRaises(ApprovalGateError):
            check_approval_gate("job_unapproved_001", approvals_path="tests/test_approvals.json")


if __name__ == "__main__":
    unittest.main()

