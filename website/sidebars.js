// @ts-check
import {execSync} from 'node:child_process';

function getConnectorVersion() {
  if (process.env.CONNECTOR_VERSION) {
    return process.env.CONNECTOR_VERSION;
  }
  try {
    const json = execSync(
      'curl -sf https://api.github.com/repos/osodevops/kafka-connect-salesforce-oss/releases/latest',
      {encoding: 'utf8', timeout: 10000, stdio: ['pipe', 'pipe', 'pipe']}
    );
    return JSON.parse(json).tag_name;
  } catch {
    return 'v0.1.0';
  }
}

const connectorVersion = getConnectorVersion();

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docsSidebar: [
    'intro',
    {
      type: 'category',
      label: 'Getting Started',
      collapsed: false,
      link: {type: 'doc', id: 'getting-started/index'},
      items: [
        'getting-started/salesforce-setup',
        'getting-started/quickstart',
      ],
    },
    {
      type: 'category',
      label: 'Connectors',
      collapsed: false,
      items: [
        'connectors/source',
        'connectors/sobject-sink',
        'connectors/platform-event-sink',
        'connectors/streaming-source',
      ],
    },
    {
      type: 'category',
      label: 'Concepts',
      items: [
        'concepts/architecture',
        'concepts/delivery-semantics',
        'concepts/offsets-and-recovery',
        'concepts/rate-limits',
      ],
    },
    {
      type: 'category',
      label: 'Reference',
      items: [
        {
          type: 'category',
          label: 'Configuration',
          items: [
            'reference/configuration/source',
            'reference/configuration/sobject-sink',
            'reference/configuration/platform-event-sink',
            'reference/configuration/streaming-source',
          ],
        },
        'reference/authentication',
        'reference/error-handling',
        'reference/schemas',
      ],
    },
    {
      type: 'category',
      label: 'Development',
      items: [
        'development/building',
        'development/local-testing',
        'development/releasing',
      ],
    },
    'migration/confluent',
    {
      type: 'html',
      value: `<a class="sidebar-version__link" href="https://github.com/osodevops/kafka-connect-salesforce-oss/releases/tag/${connectorVersion}" target="_blank" rel="noopener noreferrer"><span class="sidebar-version__label">connector</span><span class="sidebar-version__badge">${connectorVersion}</span></a>`,
      className: 'sidebar-version',
    },
  ],
};

export default sidebars;
