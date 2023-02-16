import logging

APPLY_POLICY_EVENT = "apply-policy"


class AuditLogger:
    def audit_log(self, event_name, target_id, target_type, target_label):
        logging.info(
            f"{event_name}: ID: {target_id} Type: {target_type} Label: {target_label}"
        )
