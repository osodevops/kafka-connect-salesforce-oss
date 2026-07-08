// @ts-check
import {themes as prismThemes} from 'prism-react-renderer';

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Salesforce Kafka Connector',
  tagline: 'Open-source Salesforce connectors for Apache Kafka — Pub/Sub API, Bulk API 2.0, Kafka Connect',
  favicon: 'img/oso-favicon.svg',
  url: process.env.DOCS_URL || 'https://salesforcekafkaconnector.com',
  baseUrl: process.env.DOCS_BASE_URL || '/',

  onBrokenLinks: 'throw',
  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },

  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },

  themes: [
    [
      '@easyops-cn/docusaurus-search-local',
      /** @type {import('@easyops-cn/docusaurus-search-local').PluginOptions} */
      ({
        hashed: true,
        docsRouteBasePath: '/',
        indexDocs: true,
        indexBlog: false,
        indexPages: true,
        language: ['en'],
        highlightSearchTermsOnTargetPage: true,
        searchResultLimits: 10,
        explicitSearchResultPath: true,
      }),
    ],
  ],

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          sidebarPath: './sidebars.js',
          editUrl: 'https://github.com/osodevops/kafka-connect-salesforce-oss/tree/main/website/',
          routeBasePath: '/',
        },
        blog: false,
        theme: {
          customCss: './src/css/custom.css',
        },
        sitemap: {
          changefreq: 'weekly',
          priority: 0.5,
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: 'Salesforce Kafka Connector',
        logo: {
          alt: 'OSO Logo',
          src: 'img/oso-logo.svg',
        },
        items: [
          {
            type: 'docSidebar',
            sidebarId: 'docsSidebar',
            position: 'left',
            label: 'Docs',
          },
          {
            type: 'search',
            position: 'right',
          },
          {
            href: 'https://github.com/osodevops/kafka-connect-salesforce-oss',
            label: 'GitHub',
            position: 'right',
          },
          {
            href: 'https://www.oso.sh',
            label: 'OSO',
            position: 'right',
          },
          {
            href: 'https://oso.sh/contact/',
            label: 'Contact',
            position: 'right',
          },
        ],
      },
      footer: {
        style: 'dark',
        links: [
          {
            title: 'Docs',
            items: [
              {label: 'Getting Started', to: '/getting-started'},
              {label: 'Source Connector', to: '/connectors/source'},
              {label: 'SObject Sink', to: '/connectors/sobject-sink'},
              {label: 'Migrating from Confluent', to: '/migration/confluent'},
            ],
          },
          {
            title: 'Community',
            items: [
              {label: 'GitHub', href: 'https://github.com/osodevops/kafka-connect-salesforce-oss'},
              {label: 'Issues', href: 'https://github.com/osodevops/kafka-connect-salesforce-oss/issues'},
              {label: 'Discussions', href: 'https://github.com/osodevops/kafka-connect-salesforce-oss/discussions'},
            ],
          },
          {
            title: 'OSO',
            items: [
              {label: 'oso.sh', href: 'https://www.oso.sh'},
              {label: 'Kafka Backup', href: 'https://kafkabackup.com'},
              {label: 'Contact', href: 'https://oso.sh/contact/'},
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} OSO. Built with Docusaurus.<br/>
          <small>This is an independent open-source project and is not affiliated with,
          endorsed, or sponsored by Salesforce, Inc. or the Apache Software Foundation.
          Salesforce is a trademark of Salesforce, Inc. Apache, Apache Kafka, and Kafka are
          trademarks of the Apache Software Foundation.</small>`,
      },
      prism: {
        theme: prismThemes.github,
        darkTheme: prismThemes.dracula,
        additionalLanguages: ['bash', 'yaml', 'json', 'java', 'properties'],
      },
    }),
};

export default config;
