# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import yaml

def parse_yaml_files(template_file, binding_file):
    """
    Parses template and binding YAML files and generates a combined rules YAML.

    Args:
        template_file: Path to the template YAML file.
        binding_file: Path to the binding YAML file.

    Returns:
        A list representing the combined rules YAML structure.
    """

    # Load YAML files
    with open(template_file, 'r') as template_file:
        templates = yaml.safe_load(template_file)

    with open(binding_file, 'r') as binding_file:
        bindings = yaml.safe_load(binding_file)
    
    return templates, bindings

def generate_rules(templates, bindings):
    # Initialize rules list
    rules = []

    # Iterate through bindings and create rules
    for binding in bindings.get("bindings"):
      template_name = binding.get('template_ref')
      template = next(
        template for template in templates.get("templates") if template.get("template") == template_name
      )

      for column in binding.get("columns"):
        rule = {
            "column": column,
            "dimension": template.get("rule").get("dimension"),
            "name": template.get("rule").get("name"),
            "description": template.get("rule").get("description"),
            "threshold": template.get("rule").get("threshold"),
            }
        rule.update(template.get("rule"))
        rules.append(rule)
    
    return rules

def write_output_yaml(output_file, rules):
    output_data = {
        "samplingPercent": 100,
        "rowFilter": None,
        "rules": rules,
    }

    yaml.Dumper.ignore_aliases = lambda *args : True
    with open(output_file, "w") as file:
        yaml.dump(output_data, file, default_flow_style=False, sort_keys=False)


if __name__ == "__main__":
  template_file = "/rules/templates.yaml"
  binding_file = "/rules/bindings.yaml"
  output_file = "/rules/parsed_rules_combined.yaml"

  templates, bindings = parse_yaml_files(template_file, binding_file)
  rules = generate_rules(templates, bindings)
  write_output_yaml(output_file, rules)

  print(f"Rules parsed and written to {output_file}")
  print(output_file)