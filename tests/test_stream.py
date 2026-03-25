import unittest
from unittest.mock import Mock, patch, call
from rx import create
from rx import operators as ops
from src.pipeline.stream import (
    build_pipeline,
    machines_mapping,
    properties_mapping,
    attributes_mapping
)


class TestBuildPipeline(unittest.TestCase):
    """Unit tests for the build_pipeline function"""

    def test_build_pipeline_processes_single_event(self):
        """Test that build_pipeline processes a single event correctly"""
        # Arrange
        test_event = {
            "TS": "2025-02-22T10:00:00",
            "PR": 1,
            "MC": "FB713A",
            "PS": {"T3": 30}
        }

        target_event = {
            'TIMESTAMP': '2025-02-22T10:00:00',
            'PRODUCT': 1,
            'MACHINE': 'FILLING',
            'PROPS': {'TIME': 30}
        }
        
        received_events = []
        
        def source_observable(observer, _):
            observer.on_next(test_event)
            observer.on_completed()
        
        source = create(source_observable)
        send_rich_event_mock = Mock()
        
        # Act
        pipeline = build_pipeline(source, send_rich_event_mock, None, None)
        pipeline.subscribe(
            on_next=lambda x: received_events.append(x),
            on_error=lambda e: self.fail(f"Pipeline error: {e}")
        )
        
        # TODO
        pass


class TestMappingFunctions(unittest.TestCase):
    """Unit tests for mapping functions"""

    def test_machines_mapping(self):
        """Test that machines_mapping returns correct values"""
        self.assertEqual(machines_mapping("UNS56A"), "UNSCRAMBLER")
        self.assertEqual(machines_mapping("WS964F"), "WASHER")
        self.assertEqual(machines_mapping("CPM784"), "CAPPING")

    def test_properties_mapping(self):
        """Test that properties_mapping returns correct values"""
        self.assertEqual(properties_mapping("A7"), "LITERS")
        self.assertEqual(properties_mapping("T3"), "TIME")
        self.assertEqual(properties_mapping("P6"), "POWER")

    def test_attributes_mapping(self):
        """Test that attributes_mapping returns correct values"""
        self.assertEqual(attributes_mapping("TS"), "TIMESTAMP")
        self.assertEqual(attributes_mapping("MC"), "MACHINE")
        self.assertEqual(attributes_mapping("PR"), "PRODUCT")
        self.assertEqual(attributes_mapping("PS"), "PROPS")


if __name__ == '__main__':
    unittest.main()
