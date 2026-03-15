'use strict';

const express   = require('express');
const multer    = require('multer');
const { parse } = require('csv-parse/sync');
const axios     = require('axios');
const path      = require('path');
const Anthropic = require('@anthropic-ai/sdk');

const app    = express();
const upload = multer({ storage: multer.memoryStorage() });

app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

const BASE = 'https://csv.webfleet.com/extern';

/* ─── ACTION DEFINITIONS ──────────────────────────────────────────────────── */
const ACTIONS = {

  updateVehicle: {
    label: 'Actualizar Vehículo',
    category: 'Vehículos',
    type: 'UPDATE',
    httpMethod: 'GET',
    description: 'Modifica los datos maestros de un vehículo existente (matrícula, VIN, límite de velocidad, combustible, etc.).',
    columns: [
      { name: 'objectno',           req: true,  desc: 'Nº objeto (requerido si no hay objectuid)',             ex: 'OBJ001'   },
      { name: 'objectuid',          req: false, desc: 'UID único del objeto (alternativa a objectno)',          ex: ''         },
      { name: 'denotation',         req: false, desc: 'Nombre corto / alias del vehículo (max 30 chars)',       ex: 'Furgón 01' },
      { name: 'licenseplatenumber', req: false, desc: 'Matrícula (max 20 chars)',                               ex: '1234ABC'  },
      { name: 'identnumber',        req: false, desc: 'VIN / Bastidor (max 20 chars)',                          ex: ''         },
      { name: 'speedlimit',         req: false, desc: 'Límite de velocidad en km/h (0-300)',                    ex: '120'      },
      { name: 'fueltype',           req: false, desc: '0=Diesel 1=Gasolina 2=LPG 3=CNG 4=Eléctrico 5=Híbrido', ex: '0'        },
      { name: 'fuelconsumption',    req: false, desc: 'Consumo medio en L/100km (0-100)',                       ex: '8'        },
      { name: 'fueltanksize',       req: false, desc: 'Capacidad del depósito en litros',                       ex: '60'       },
      { name: 'vehiclecolor',       req: false, desc: 'white / grey / black / red / orange / yellow / green / blue', ex: 'white' },
      { name: 'description',        req: false, desc: 'Descripción libre (max 500 chars)',                      ex: ''         },
      { name: 'odometer',           req: false, desc: 'Odómetro en metros',                                     ex: ''         },
      { name: 'netweight',          req: false, desc: 'Tara en kg',                                             ex: ''         },
      { name: 'maxweight',          req: false, desc: 'Peso máximo en kg',                                      ex: ''         },
      { name: 'power',              req: false, desc: 'Potencia en kW',                                         ex: ''         },
      { name: 'enginesize',         req: false, desc: 'Cilindrada en cc',                                       ex: ''         },
      { name: 'manufacturedyear',   req: false, desc: 'Año de fabricación (4 dígitos)',                         ex: '2023'     },
      { name: 'externalid',         req: false, desc: 'ID externo en tu sistema',                               ex: ''         },
    ],
  },

  insertDriverExtern: {
    label: 'Insertar Conductor',
    category: 'Conductores',
    type: 'INSERT',
    httpMethod: 'GET',
    description: 'Crea un nuevo conductor en la cuenta de Webfleet.',
    columns: [
      { name: 'driverno',            req: true,  desc: 'Nº conductor único (max 15 chars)',                     ex: 'DRV001'           },
      { name: 'name',                req: true,  desc: 'Nombre completo (max 50 chars)',                        ex: 'Juan García'      },
      { name: 'name2',               req: false, desc: 'Segundo nombre / apellido (max 50 chars)',              ex: ''                 },
      { name: 'company',             req: false, desc: 'Empresa (max 250 chars)',                               ex: ''                 },
      { name: 'telmobile',           req: false, desc: 'Teléfono móvil (max 50 chars)',                         ex: '+34612345678'     },
      { name: 'telprivate',          req: false, desc: 'Teléfono privado (max 50 chars)',                       ex: ''                 },
      { name: 'email',               req: false, desc: 'Email (max 50 chars)',                                  ex: 'juan@empresa.com' },
      { name: 'pin',                 req: false, desc: 'PIN numérico de acceso (max 20 dígitos)',               ex: '1234'             },
      { name: 'dt_cardid',           req: false, desc: 'ID tarjeta tacógrafo (max 16 chars)',                   ex: ''                 },
      { name: 'dt_cardcountry',      req: false, desc: 'País de la tarjeta (ISO 3166-1, 2 chars)',              ex: 'ES'               },
      { name: 'country',             req: false, desc: 'País domicilio (ISO 3166-1, 2 chars)',                  ex: 'ES'               },
      { name: 'zip',                 req: false, desc: 'Código postal (max 10 chars)',                          ex: '08001'            },
      { name: 'city',                req: false, desc: 'Ciudad (max 250 chars)',                                ex: 'Barcelona'        },
      { name: 'street',              req: false, desc: 'Calle (max 250 chars)',                                 ex: ''                 },
      { name: 'license_number',      req: false, desc: 'Nº carné de conducir (max 20 chars)',                   ex: ''                 },
      { name: 'license_country',     req: false, desc: 'País del carné (2 chars)',                              ex: 'ES'               },
      { name: 'license_expiry_date', req: false, desc: 'Vencimiento carné (dd/MM/yyyy)',                        ex: '31/12/2030'       },
    ],
  },

  updateDriverExtern: {
    label: 'Actualizar Conductor',
    category: 'Conductores',
    type: 'UPDATE',
    httpMethod: 'GET',
    description: 'Modifica los datos de un conductor existente. Solo se actualizan los campos con valor.',
    columns: [
      { name: 'driverno',            req: true,  desc: 'Nº conductor (requerido si no hay driveruid)',          ex: 'DRV001' },
      { name: 'driveruid',           req: false, desc: 'UID único del conductor (alternativa a driverno)',      ex: ''       },
      { name: 'name',                req: false, desc: 'Nombre completo (max 50 chars)',                        ex: ''       },
      { name: 'name2',               req: false, desc: 'Segundo nombre (max 50 chars)',                         ex: ''       },
      { name: 'company',             req: false, desc: 'Empresa (max 250 chars)',                               ex: ''       },
      { name: 'telmobile',           req: false, desc: 'Teléfono móvil (max 50 chars)',                         ex: ''       },
      { name: 'email',               req: false, desc: 'Email (max 50 chars)',                                  ex: ''       },
      { name: 'pin',                 req: false, desc: 'PIN numérico',                                          ex: ''       },
      { name: 'dt_cardid',           req: false, desc: 'ID tarjeta tacógrafo (max 16 chars)',                   ex: ''       },
      { name: 'dt_cardcountry',      req: false, desc: 'País de la tarjeta (ISO 3166-1)',                       ex: ''       },
      { name: 'license_number',      req: false, desc: 'Nº carné de conducir (max 20 chars)',                   ex: ''       },
      { name: 'license_expiry_date', req: false, desc: 'Vencimiento carné (dd/MM/yyyy)',                        ex: ''       },
      { name: 'description',         req: false, desc: 'Notas adicionales (max 4000 chars)',                    ex: ''       },
    ],
  },

  insertAddressExtern: {
    label: 'Insertar Dirección',
    category: 'Direcciones',
    type: 'INSERT',
    httpMethod: 'GET',
    description: 'Añade un nuevo POI / dirección a la cuenta. Se puede geocodificar automáticamente si se omiten las coordenadas.',
    columns: [
      { name: 'addrnr',      req: true,  desc: 'Nº dirección único (max 10 chars)',                              ex: 'ADDR001'       },
      { name: 'addrname1',   req: true,  desc: 'Nombre de la dirección (max 50 chars)',                          ex: 'Sede Central'  },
      { name: 'addrname2',   req: false, desc: 'Nombre 2 (max 50 chars)',                                        ex: ''              },
      { name: 'addrstreet',  req: false, desc: 'Calle (max 50 chars)',                                           ex: 'Calle Mayor 1' },
      { name: 'addrzip',     req: false, desc: 'Código postal (max 10 chars)',                                   ex: '28001'         },
      { name: 'addrcity',    req: false, desc: 'Ciudad (max 50 chars)',                                          ex: 'Madrid'        },
      { name: 'addrcountry', req: false, desc: 'País (ISO 3166-1 alfa-2)',                                       ex: 'ES'            },
      { name: 'contact',     req: false, desc: 'Persona de contacto (max 50 chars)',                             ex: ''              },
      { name: 'telmobile',   req: false, desc: 'Teléfono móvil (max 20 chars)',                                  ex: ''              },
      { name: 'teloffice',   req: false, desc: 'Teléfono oficina (max 20 chars)',                                ex: ''              },
      { name: 'mailaddr',    req: false, desc: 'Email (max 254 chars)',                                          ex: ''              },
      { name: 'positiony',   req: false, desc: 'Latitud en microgrados WGS84 (ej: 40416775 = 40.416775°)',       ex: '40416775'      },
      { name: 'positionx',   req: false, desc: 'Longitud en microgrados WGS84 (ej: -3703790 = -3.703790°)',      ex: '-3703790'      },
      { name: 'radius',      req: false, desc: 'Radio de la zona en metros (defecto: 250)',                      ex: '250'           },
      { name: 'addrinfo',    req: false, desc: 'Información adicional (max 1000 chars)',                         ex: ''              },
      { name: 'visible',     req: false, desc: 'Visible en mapa (true / false)',                                 ex: 'true'          },
      { name: 'color',       req: false, desc: 'Color icono: brightblue / brightgreen / darkred / yellow / ...',ex: 'brightblue'    },
      { name: 'addrgrpname', req: false, desc: 'Grupo de direcciones (max 30 chars)',                            ex: ''              },
    ],
  },

  updateAddressExtern: {
    label: 'Actualizar Dirección',
    category: 'Direcciones',
    type: 'UPDATE',
    httpMethod: 'GET',
    description: 'Modifica los datos de un POI / dirección existente.',
    columns: [
      { name: 'addrnr',      req: true,  desc: 'Nº dirección a modificar (requerido)',                          ex: 'ADDR001' },
      { name: 'addrname1',   req: false, desc: 'Nombre de la dirección (max 50 chars)',                          ex: ''        },
      { name: 'addrstreet',  req: false, desc: 'Calle (max 50 chars)',                                           ex: ''        },
      { name: 'addrzip',     req: false, desc: 'Código postal (max 10 chars)',                                   ex: ''        },
      { name: 'addrcity',    req: false, desc: 'Ciudad (max 50 chars)',                                          ex: ''        },
      { name: 'addrcountry', req: false, desc: 'País (ISO 3166-1)',                                              ex: ''        },
      { name: 'contact',     req: false, desc: 'Persona de contacto (max 50 chars)',                             ex: ''        },
      { name: 'telmobile',   req: false, desc: 'Teléfono móvil (max 20 chars)',                                  ex: ''        },
      { name: 'mailaddr',    req: false, desc: 'Email (max 254 chars)',                                          ex: ''        },
      { name: 'positiony',   req: false, desc: 'Latitud en microgrados WGS84',                                   ex: ''        },
      { name: 'positionx',   req: false, desc: 'Longitud en microgrados WGS84',                                  ex: ''        },
      { name: 'radius',      req: false, desc: 'Radio en metros',                                                ex: ''        },
      { name: 'visible',     req: false, desc: 'Visible en mapa (true / false)',                                 ex: ''        },
      { name: 'addrinfo',    req: false, desc: 'Información adicional (max 1000 chars)',                         ex: ''        },
    ],
  },

  sendOrderExtern: {
    label: 'Enviar Orden',
    category: 'Órdenes',
    type: 'INSERT',
    httpMethod: 'POST',
    description: 'Envía una nueva orden de trabajo a un vehículo o conductor.',
    columns: [
      { name: 'objectno',       req: true,  desc: 'Nº objeto destino (o usar objectuid)',                        ex: 'OBJ001'                },
      { name: 'objectuid',      req: false, desc: 'UID objeto destino (alternativa a objectno)',                  ex: ''                      },
      { name: 'orderid',        req: true,  desc: 'ID de orden único en la cuenta (max 20 bytes UTF-8)',          ex: 'ORD001'                },
      { name: 'ordertext',      req: true,  desc: 'Texto de la orden (max 1000 chars)',                           ex: 'Recogida en almacén A' },
      { name: 'ordertype',      req: false, desc: '1=Servicio  2=Recogida  3=Entrega',                            ex: '3'                     },
      { name: 'useorderstates', req: false, desc: 'Estados de workflow a activar (ej: 202,203,204,205)',          ex: ''                      },
    ],
  },

  updateOrderExtern: {
    label: 'Actualizar Orden',
    category: 'Órdenes',
    type: 'UPDATE',
    httpMethod: 'POST',
    description: 'Modifica el texto de una orden activa ya enviada al vehículo.',
    columns: [
      { name: 'orderid',   req: true, desc: 'ID de la orden a modificar (max 20 chars)',         ex: 'ORD001'         },
      { name: 'ordertext', req: true, desc: 'Nuevo texto de la orden (max 1000 chars)',           ex: 'Texto revisado' },
    ],
  },

  cancelOrderExtern: {
    label: 'Cancelar Orden',
    category: 'Órdenes',
    type: 'CANCEL',
    httpMethod: 'GET',
    description: 'Cancela una orden activa. El vehículo recibirá la cancelación.',
    columns: [
      { name: 'orderid', req: true, desc: 'ID de la orden a cancelar (max 20 chars)', ex: 'ORD001' },
    ],
  },

  insertArea: {
    label: 'Insertar Geocerca',
    category: 'Geocercas',
    type: 'INSERT',
    httpMethod: 'POST',
    description: 'Crea una nueva área de geofencing (círculo o rectángulo). Para polígonos usar la interfaz web.',
    columns: [
      { name: 'areano',           req: true,  desc: 'Nº área único (max 50 chars)',                               ex: 'AREA001'  },
      { name: 'areaname',         req: true,  desc: 'Nombre del área (max 254 chars)',                            ex: 'Zona Norte' },
      { name: 'type',             req: true,  desc: '1=Rectángulo  2=Círculo  3=Polígono  4=Corredor',            ex: '2'        },
      { name: 'latitude',         req: false, desc: 'Latitud centro en microgrados (necesario para tipo 1 y 2)',  ex: '40416775' },
      { name: 'longitude',        req: false, desc: 'Longitud centro en microgrados (necesario para tipo 1 y 2)', ex: '-3703790' },
      { name: 'radius',           req: false, desc: 'Radio en metros (obligatorio si type=2)',                    ex: '500'      },
      { name: 'width',            req: false, desc: 'Anchura en metros (type=1 o 4)',                             ex: ''         },
      { name: 'height',           req: false, desc: 'Altura en metros (type=1)',                                  ex: ''         },
      { name: 'active',           req: false, desc: 'Activar al crear (true / false, defecto: true)',             ex: 'true'     },
      { name: 'notificationmode', req: false, desc: '0=dentro/fuera  1=entrada/salida (defecto: 1)',              ex: '1'        },
      { name: 'eventlevel_enter', req: false, desc: 'Nivel de alerta en entrada (1-5; 5=Alarm3)',                 ex: '3'        },
      { name: 'eventlevel_leave', req: false, desc: 'Nivel de alerta en salida (1-5)',                            ex: '3'        },
      { name: 'validfrom',        req: false, desc: 'Válida desde (dd/MM/yyyy)',                                  ex: ''         },
      { name: 'validto',          req: false, desc: 'Válida hasta (dd/MM/yyyy)',                                  ex: ''         },
      { name: 'color',            req: false, desc: 'Color en hex RGB (ej: 3366FF)',                              ex: '3366FF'   },
    ],
  },

  insertMaintenanceSchedule: {
    label: 'Insertar Mantenimiento',
    category: 'Mantenimiento',
    type: 'INSERT',
    httpMethod: 'GET',
    description: 'Crea un programa de mantenimiento periódico para un vehículo (ITV, aceite, neumáticos, etc.).',
    columns: [
      { name: 'objectno',             req: true,  desc: 'Nº objeto (o usar objectuid)',                           ex: 'OBJ001'        },
      { name: 'objectuid',            req: false, desc: 'UID del objeto',                                         ex: ''              },
      { name: 'schedulename',         req: true,  desc: 'Nombre del programa (max 100 chars)',                    ex: 'Revisión ITV'  },
      { name: 'scheduletype',         req: false, desc: '0=Personalizado 1=Aceite 2=Neumáticos ... (0-26)',        ex: '0'             },
      { name: 'scheduledescription',  req: false, desc: 'Descripción (max 2000 chars)',                           ex: ''              },
      { name: 'ruletype',             req: false, desc: '0=Una vez  1=Continuo fijo  2=Secuencial',               ex: '1'             },
      { name: 'intervaltime',         req: false, desc: 'Intervalo de tiempo en meses',                           ex: '12'            },
      { name: 'intervalodometer',     req: false, desc: 'Intervalo de odómetro en metros (ej: 30000000=30.000km)',ex: '30000000'      },
      { name: 'remindingtime',        req: false, desc: 'Recordatorio previo en días',                            ex: '30'            },
      { name: 'remindingodometer',    req: false, desc: 'Recordatorio previo en metros',                          ex: '1000000'       },
      { name: 'plannedexectime',      req: false, desc: 'Fecha de primera ejecución (dd/MM/yyyy)',                ex: ''              },
    ],
  },

  insertUser: {
    label: 'Insertar Usuario',
    category: 'Usuarios',
    type: 'INSERT',
    httpMethod: 'GET',
    description: 'Crea un nuevo usuario de acceso a la cuenta de Webfleet.',
    columns: [
      { name: 'username',  req: true,  desc: 'Nombre de usuario (max 50 chars)',                                  ex: 'usuario01'        },
      { name: 'password',  req: true,  desc: 'Contraseña del nuevo usuario',                                      ex: 'Pass@1234'        },
      { name: 'realname',  req: false, desc: 'Nombre real (max 50 chars)',                                        ex: 'Juan García'      },
      { name: 'company',   req: false, desc: 'Empresa (max 50 chars)',                                            ex: ''                 },
      { name: 'email',     req: false, desc: 'Email (max 255 chars)',                                             ex: 'juan@empresa.com' },
      { name: 'profile',   req: false, desc: 'Perfil: guest / standard / expert / admin',                        ex: 'standard'         },
      { name: 'validfrom', req: false, desc: 'Válido desde (dd/MM/yyyy)',                                         ex: ''                 },
      { name: 'validto',   req: false, desc: 'Válido hasta (dd/MM/yyyy)',                                         ex: ''                 },
      { name: 'userinfo',  req: false, desc: 'Información adicional (max 4000 chars)',                            ex: ''                 },
    ],
  },

  updateUser: {
    label: 'Actualizar Usuario',
    category: 'Usuarios',
    type: 'UPDATE',
    httpMethod: 'GET',
    description: 'Modifica los datos de un usuario existente en la cuenta.',
    columns: [
      { name: 'username',  req: true,  desc: 'Nombre de usuario a modificar (max 50 chars)',                     ex: 'usuario01' },
      { name: 'realname',  req: false, desc: 'Nombre real (max 50 chars)',                                        ex: ''          },
      { name: 'company',   req: false, desc: 'Empresa (max 50 chars)',                                            ex: ''          },
      { name: 'email',     req: false, desc: 'Email (max 255 chars)',                                             ex: ''          },
      { name: 'profile',   req: false, desc: 'Perfil: guest / standard / expert / admin',                        ex: ''          },
      { name: 'validfrom', req: false, desc: 'Válido desde (dd/MM/yyyy)',                                         ex: ''          },
      { name: 'validto',   req: false, desc: 'Válido hasta (dd/MM/yyyy)',                                         ex: ''          },
      { name: 'userinfo',  req: false, desc: 'Información adicional (max 4000 chars)',                            ex: ''          },
    ],
  },

  insertObjectGroupExtern: {
    label: 'Crear Grupo de Vehículos',
    category: 'Grupos de Vehículos',
    type: 'INSERT',
    httpMethod: 'GET',
    description: 'Crea un nuevo grupo de vehículos en la cuenta. Los grupos permiten filtrar y organizar la flota.',
    columns: [
      { name: 'objectgroupname', req: true,  desc: 'Nombre del grupo de vehículos (único en la cuenta, max 50 chars)', ex: 'Zona Norte'   },
      { name: 'description',     req: false, desc: 'Descripción del grupo (max 500 chars)',                            ex: 'Vehículos asignados a la zona norte' },
    ],
  },

  insertObjectGroupObjectExtern: {
    label: 'Añadir Vehículo a Grupo',
    category: 'Grupos de Vehículos',
    type: 'INSERT',
    httpMethod: 'GET',
    description: 'Asigna un vehículo existente a un grupo de vehículos. Un vehículo puede pertenecer a varios grupos.',
    columns: [
      { name: 'objectgroupname', req: true,  desc: 'Nombre del grupo de vehículos al que se añade el vehículo',       ex: 'Zona Norte' },
      { name: 'objectno',        req: true,  desc: 'Nº del objeto/vehículo (requerido si no hay objectuid)',           ex: 'OBJ001'     },
      { name: 'objectuid',       req: false, desc: 'UID único del objeto (alternativa a objectno)',                    ex: ''           },
    ],
  },

  insertDriverGroupExtern: {
    label: 'Crear Grupo de Conductores',
    category: 'Grupos de Conductores',
    type: 'INSERT',
    httpMethod: 'GET',
    description: 'Crea un nuevo grupo de conductores en la cuenta. Los grupos permiten segmentar y filtrar conductores.',
    columns: [
      { name: 'drivergroupname', req: true,  desc: 'Nombre del grupo de conductores (único en la cuenta, max 50 chars)', ex: 'Turno Mañana' },
      { name: 'description',     req: false, desc: 'Descripción del grupo (max 500 chars)',                               ex: 'Conductores del turno de mañana' },
    ],
  },

  insertDriverGroupDriverExtern: {
    label: 'Añadir Conductor a Grupo',
    category: 'Grupos de Conductores',
    type: 'INSERT',
    httpMethod: 'GET',
    description: 'Asigna un conductor existente a un grupo de conductores. Un conductor puede pertenecer a varios grupos.',
    columns: [
      { name: 'drivergroupname', req: true,  desc: 'Nombre del grupo de conductores al que se asigna el conductor',   ex: 'Turno Mañana' },
      { name: 'driverno',        req: true,  desc: 'Nº del conductor (requerido si no hay driveruid)',                 ex: 'DRV001'       },
      { name: 'driveruid',       req: false, desc: 'UID único del conductor (alternativa a driverno)',                 ex: ''             },
    ],
  },
};

/* ─── ROUTES ──────────────────────────────────────────────────────────────── */

// Return full actions config to frontend
app.get('/api/actions', (_req, res) => res.json(ACTIONS));

// Download CSV template for a given action
app.get('/api/template/:action', (req, res) => {
  const cfg = ACTIONS[req.params.action];
  if (!cfg) return res.status(404).json({ error: 'Acción no encontrada' });

  const header  = cfg.columns.map(c => c.name).join(',');
  const example = cfg.columns.map(c => `"${(c.ex || '').replace(/"/g, '""')}"`).join(',');

  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="plantilla_${req.params.action}.csv"`);
  res.send('\uFEFF' + header + '\n' + example + '\n'); // BOM for Excel
});

// Execute bulk operation from uploaded CSV
app.post('/api/execute', upload.single('csvfile'), async (req, res) => {
  const { action, account, username, password, apikey } = req.body;

  if (!req.file)          return res.status(400).json({ error: 'Falta el archivo CSV' });
  if (!ACTIONS[action])   return res.status(400).json({ error: 'Acción no válida' });
  if (!account || !apikey) return res.status(400).json({ error: 'Faltan credenciales (account / apikey)' });

  const rawCsv = req.file.buffer.toString('utf-8').replace(/^\uFEFF/, ''); // strip BOM

  let records;
  try {
    records = parse(rawCsv, { columns: true, skip_empty_lines: true, trim: true });
  } catch (e) {
    return res.status(400).json({ error: `CSV inválido: ${e.message}` });
  }

  if (records.length === 0) return res.status(400).json({ error: 'El CSV no contiene filas de datos' });

  const cfg     = ACTIONS[action];
  const results = [];

  for (let i = 0; i < records.length; i++) {
    const row = records[i];

    // Fixed credentials + action + row values (skip empty)
    const params = { account, username, password, apikey, outputformat: 'json', useUTF8: 'true', lang: 'es', action };
    for (const [k, v] of Object.entries(row)) {
      if (v !== '' && v != null) params[k] = v;
    }

    try {
      let resp;
      if (cfg.httpMethod === 'POST') {
        resp = await axios.post(BASE, new URLSearchParams(params).toString(), {
          headers: { 'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8' },
        });
      } else {
        resp = await axios.get(BASE, { params });
      }

      // Webfleet signals errors via response headers
      const errCode = resp.headers['x-webfleet-errorcode'];
      const errMsg  = resp.headers['x-webfleet-errormessage'];
      const isErr   = errCode && parseInt(errCode) !== 0;

      results.push({
        row: i + 1,
        data: row,
        success: !isErr,
        errorCode: isErr ? errCode : null,
        errorMsg:  isErr ? decodeURIComponent(errMsg || '') : null,
        response:  resp.data,
      });
    } catch (e) {
      const errCode = e.response?.headers?.['x-webfleet-errorcode'];
      const errMsg  = e.response?.headers?.['x-webfleet-errormessage'] || e.message;
      results.push({ row: i + 1, data: row, success: false, errorCode, errorMsg: errMsg });
    }

    // 100ms pause between calls to respect rate limits
    if (i < records.length - 1) await new Promise(r => setTimeout(r, 100));
  }

  res.json({
    total:   records.length,
    success: results.filter(r =>  r.success).length,
    errors:  results.filter(r => !r.success).length,
    results,
  });
});

/* ─── SNAPSHOT / ROLLBACK ROUTE ──────────────────────────────────────────── */

app.post('/api/snapshot', upload.single('csvfile'), async (req, res) => {
  const { action, account, username, password, apikey } = req.body;
  const map = READ_MAP[action];
  if (!map) return res.json({ supported: false });

  const rawCsv = req.file.buffer.toString('utf-8').replace(/^\uFEFF/, '');
  let csvRows;
  try {
    csvRows = parse(rawCsv, { columns: true, skip_empty_lines: true, trim: true });
  } catch (e) {
    return res.status(400).json({ error: 'CSV inválido: ' + e.message });
  }

  const csvIds  = new Set(csvRows.map(r => String(r[map.idField] || '').trim()).filter(Boolean));
  const csvCols = csvRows[0] ? Object.keys(csvRows[0]) : [];

  const params = { account, username, password, apikey, outputformat: 'json', useUTF8: 'true', lang: 'es', action: map.readAction };
  try {
    const resp     = await axios.get(BASE, { params });
    const errCode  = resp.headers['x-webfleet-errorcode'];
    if (errCode && parseInt(errCode) !== 0) throw new Error('Error ' + errCode + ': ' + decodeURIComponent(resp.headers['x-webfleet-errormessage'] || ''));
    const allRecords = Array.isArray(resp.data) ? resp.data : [];

    // Keep only records that are in the CSV, with only the CSV columns
    const snapshot = allRecords
      .filter(r => csvIds.has(String(r[map.idField] || '').trim()))
      .map(r => {
        const row = {};
        csvCols.forEach(col => { row[col] = r[col] !== undefined ? String(r[col]) : ''; });
        return row;
      });

    res.json({ supported: true, snapshot, idField: map.idField, columns: csvCols, action, takenAt: new Date().toISOString() });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ─── SYNC ANALYSIS ROUTE ─────────────────────────────────────────────────── */

const SYNC_ENTITIES = {
  vehicle: {
    label:        'Vehículos',
    readAction:   'showObjectReportExtern',
    insertAction: null,   // Webfleet doesn't expose insertVehicle via this API
    updateAction: 'updateVehicle',
    idField:      'objectno',
  },
  driver: {
    label:        'Conductores',
    readAction:   'showDriverExtern',
    insertAction: 'insertDriverExtern',
    updateAction: 'updateDriverExtern',
    idField:      'driverno',
  },
  address: {
    label:        'Direcciones',
    readAction:   'showAddressExtern',
    insertAction: 'insertAddressExtern',
    updateAction: 'updateAddressExtern',
    idField:      'addrnr',
  },
};

app.post('/api/sync-analyze', upload.single('csvfile'), async (req, res) => {
  const { entity, account, username, password, apikey } = req.body;
  const cfg = SYNC_ENTITIES[entity];
  if (!cfg) return res.status(400).json({ error: 'Entidad no soportada' });
  if (!req.file) return res.status(400).json({ error: 'Falta el archivo' });

  const rawCsv = req.file.buffer.toString('utf-8').replace(/^\uFEFF/, '');
  let sourceRows;
  try {
    sourceRows = parse(rawCsv, { columns: true, skip_empty_lines: true, trim: true });
  } catch (e) {
    return res.status(400).json({ error: 'CSV inválido: ' + e.message });
  }

  const params = { account, username, password, apikey, outputformat: 'json', useUTF8: 'true', lang: 'es', action: cfg.readAction };
  let currentRecords;
  try {
    const resp    = await axios.get(BASE, { params });
    const errCode = resp.headers['x-webfleet-errorcode'];
    if (errCode && parseInt(errCode) !== 0) throw new Error('Error ' + errCode + ': ' + decodeURIComponent(resp.headers['x-webfleet-errormessage'] || ''));
    currentRecords = Array.isArray(resp.data) ? resp.data : [];
  } catch (e) {
    return res.status(500).json({ error: 'No se pudo leer Webfleet: ' + e.message });
  }

  const index = {};
  currentRecords.forEach(r => {
    const key = String(r[cfg.idField] || '').trim();
    if (key) index[key] = r;
  });

  const toInsert = [], toUpdate = [], toSkip = [];

  sourceRows.forEach((row, i) => {
    const id      = String(row[cfg.idField] || '').trim();
    const current = index[id];

    if (!current) {
      toInsert.push({ row: i + 1, id, data: row });
    } else {
      const changes = Object.entries(row)
        .filter(([k, v]) => v && String(v).trim() !== String(current[k] || '').trim())
        .map(([k, v]) => ({ field: k, currentVal: String(current[k] || '') || '—', newVal: String(v).trim() }));

      if (changes.length === 0) toSkip.push({ row: i + 1, id, data: row });
      else                       toUpdate.push({ row: i + 1, id, data: row, changes });
    }
  });

  res.json({
    entity, label: cfg.label,
    insertAction: cfg.insertAction,
    updateAction: cfg.updateAction,
    idField:      cfg.idField,
    toInsert, toUpdate, toSkip,
    summary: { total: sourceRows.length, toInsert: toInsert.length, toUpdate: toUpdate.length, toSkip: toSkip.length },
  });
});

/* ─── DIFF / PREVIEW-CHANGES ROUTE ───────────────────────────────────────── */

// Maps each UPDATE action to its corresponding Webfleet read endpoint
const READ_MAP = {
  updateVehicle:      { readAction: 'showObjectReportExtern', idField: 'objectno'  },
  updateDriverExtern: { readAction: 'showDriverExtern',       idField: 'driverno'  },
  updateAddressExtern:{ readAction: 'showAddressExtern',      idField: 'addrnr'    },
  updateUser:         { readAction: 'showUserExtern',         idField: 'username'  },
};

app.post('/api/diff', upload.single('csvfile'), async (req, res) => {
  const { action, account, username, password, apikey } = req.body;

  const map = READ_MAP[action];
  if (!map) return res.json({ supported: false });

  if (!req.file)          return res.status(400).json({ error: 'Falta el archivo CSV' });
  if (!account || !apikey) return res.status(400).json({ error: 'Faltan credenciales' });

  const rawCsv = req.file.buffer.toString('utf-8').replace(/^\uFEFF/, '');
  let newRows;
  try {
    newRows = parse(rawCsv, { columns: true, skip_empty_lines: true, trim: true });
  } catch (e) {
    return res.status(400).json({ error: 'CSV inválido: ' + e.message });
  }

  // Fetch ALL current records from Webfleet
  const params = { account, username, password, apikey, outputformat: 'json', useUTF8: 'true', lang: 'es', action: map.readAction };
  let currentRecords;
  try {
    const resp = await axios.get(BASE, { params });
    const errCode = resp.headers['x-webfleet-errorcode'];
    if (errCode && parseInt(errCode) !== 0) {
      throw new Error('Error ' + errCode + ': ' + decodeURIComponent(resp.headers['x-webfleet-errormessage'] || ''));
    }
    currentRecords = Array.isArray(resp.data) ? resp.data : [];
  } catch (e) {
    return res.status(500).json({ error: 'No se pudo leer el estado actual: ' + e.message });
  }

  // Index current records by identifier field
  const index = {};
  currentRecords.forEach(r => {
    const key = r[map.idField] || r[map.idField.replace('no','nr')]; // tolerate addrnr vs addrnr
    if (key) index[String(key).trim()] = r;
  });

  // Build diff per row
  const diff = newRows.map((newRow, i) => {
    const id      = String(newRow[map.idField] || '').trim();
    const current = id ? (index[id] || null) : null;

    const changes   = [];
    const unchanged = [];

    Object.entries(newRow).forEach(([field, newVal]) => {
      if (!newVal || newVal === '') return;
      const curVal = current ? String(current[field] || '').trim() : '';
      if (String(newVal).trim() !== curVal) {
        changes.push({ field, currentVal: curVal || '—', newVal: String(newVal).trim() });
      } else {
        unchanged.push(field);
      }
    });

    return { row: i + 1, id, found: !!current, changes, unchangedCount: unchanged.length };
  });

  const totalChanges = diff.reduce((s, r) => s + r.changes.length, 0);
  const notFound     = diff.filter(r => !r.found).length;

  res.json({ supported: true, idField: map.idField, diff, totalChanges, notFound });
});

/* ─── AI AGENT ROUTE ──────────────────────────────────────────────────────── */

app.post('/api/agent-interpret', async (req, res) => {
  const { rawData, anthropicApiKey } = req.body;
  if (!rawData)         return res.status(400).json({ error: 'Falta el contenido a interpretar' });
  if (!anthropicApiKey) return res.status(400).json({ error: 'Falta la API key de Anthropic' });

  const actionsSummary = Object.entries(ACTIONS).map(([id, a]) => ({
    id,
    label: a.label,
    category: a.category,
    description: a.description,
    fields: a.columns.map(c => ({ name: c.name, required: c.req, description: c.desc, example: c.ex })),
  }));

  const system = `Eres un agente experto en la API Webfleet.connect. Recibirás datos de clientes en cualquier formato (CSV, TSV, texto libre, tabla pegada de Excel, JSON, etc.) y debes:
1. Detectar qué operación(es) de Webfleet corresponden
2. Mapear columnas del input a los campos exactos de la API
3. Transformar valores si es necesario (ej: "blanco"→"white", "diesel"→"0", fechas a dd/MM/yyyy, nombres de tipo de combustible a código numérico)
4. Identificar problemas: campos requeridos faltantes, valores fuera de rango, posibles duplicados
5. Si hay datos de múltiples tipos de operación, crear un batch separado por cada tipo

IMPORTANTE: Responde ÚNICAMENTE con JSON válido, sin texto adicional, sin bloques de código markdown.`;

  const userMsg = `ACCIONES DISPONIBLES:
${JSON.stringify(actionsSummary, null, 2)}

DATOS DEL CLIENTE:
${rawData}

Devuelve un JSON con esta estructura exacta:
{
  "summary": "descripción breve de lo detectado",
  "batches": [
    {
      "action": "id_accion_webfleet",
      "label": "nombre legible",
      "confidence": 0.95,
      "reasoning": "explicación del mapeo realizado",
      "column_mapping": { "NombreColumnaOriginal": "campo_api" },
      "issues": [
        { "type": "error|warning|info", "message": "descripción" }
      ],
      "rows": [
        {
          "id": 1,
          "original": { "columna_original": "valor" },
          "mapped": { "campo_api": "valor_procesado" },
          "row_issues": [{ "type": "warning|error", "field": "campo", "message": "msg" }]
        }
      ]
    }
  ]
}`;

  try {
    const client  = new Anthropic({ apiKey: anthropicApiKey });
    const message = await client.messages.create({
      model:   'claude-opus-4-6',
      max_tokens: 4096,
      system,
      messages: [{ role: 'user', content: userMsg }],
    });

    const text    = message.content[0].text.trim();
    const jsonStr = text.startsWith('{') ? text : (text.match(/```(?:json)?\n?([\s\S]*?)\n?```/)?.[1] || text);
    res.json(JSON.parse(jsonStr));
  } catch (e) {
    if (e instanceof SyntaxError) {
      res.status(500).json({ error: 'El agente devolvió una respuesta inválida. Inténtalo de nuevo.' });
    } else {
      res.status(500).json({ error: e.message });
    }
  }
});

/* ─── FLEET DASHBOARD ROUTES ──────────────────────────────────────────────── */

// Fetch showObjectReportExtern + showVehicleReportExtern in parallel
app.post('/api/fleet-report', async (req, res) => {
  const { account, username, password, apikey } = req.body;
  if (!account || !apikey) return res.status(400).json({ error: 'Faltan credenciales (account / apikey)' });

  const base = { account, username, password, apikey, outputformat: 'json', useUTF8: 'true', lang: 'es' };

  const checkErr = (r) => {
    const code = r.headers['x-webfleet-errorcode'];
    if (code && parseInt(code) !== 0) {
      throw new Error('Error ' + code + ': ' + decodeURIComponent(r.headers['x-webfleet-errormessage'] || 'sin descripción'));
    }
    return Array.isArray(r.data) ? r.data : (r.data ? [r.data] : []);
  };

  try {
    const [objResp, vehResp] = await Promise.all([
      axios.get(BASE, { params: { ...base, action: 'showObjectReportExtern' } }),
      axios.get(BASE, { params: { ...base, action: 'showVehicleReportExtern' } }),
    ]);
    res.json({ objects: checkErr(objResp), vehicles: checkErr(vehResp) });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Generate AI insights via Claude
app.post('/api/fleet-insights', async (req, res) => {
  const { fleetData, anthropicApiKey } = req.body;
  if (!anthropicApiKey) return res.status(400).json({ error: 'Falta la API key de Anthropic' });

  // Build a compact summary to avoid sending full raw data
  const objects  = fleetData.objects  || [];
  const vehicles = fleetData.vehicles || [];

  const moving  = objects.filter(o => (o.pos_speed || 0) > 0).length;
  const stopped = objects.length - moving;

  const vehicleTypes = {};
  objects.forEach(o => {
    const t = o.vehicletype || o.vehicle_type || 'Desconocido';
    vehicleTypes[t] = (vehicleTypes[t] || 0) + 1;
  });

  const speedBuckets = { detenido: 0, lento_0_60: 0, medio_60_100: 0, rapido_100: 0 };
  objects.forEach(o => {
    const s = o.pos_speed || 0;
    if      (s === 0)   speedBuckets.detenido++;
    else if (s < 6000)  speedBuckets.lento_0_60++;    // pos_speed in 1/100 km/h
    else if (s < 10000) speedBuckets.medio_60_100++;
    else                speedBuckets.rapido_100++;
  });

  const topKm = vehicles
    .map(v => ({ name: v.objectname || v.objectno, km: Math.round((v.mileage || 0) / 1000) }))
    .sort((a, b) => b.km - a.km)
    .slice(0, 10);

  const summary = {
    totalVehiculos: objects.length,
    enMovimiento:   moving,
    detenidos:      stopped,
    tiposVehiculo:  vehicleTypes,
    distribucionVelocidad: speedBuckets,
    top10PorKilometraje: topKm,
    totalVehiculosConOdometro: vehicles.length,
  };

  const prompt = `Eres un analista senior de datos y experto en gestión de flotas de vehículos con más de 15 años de experiencia en operaciones logísticas y transporte.

Analiza los siguientes datos en tiempo real de la flota y proporciona un análisis ejecutivo. Estructura tu respuesta con estos apartados exactos en markdown:

## Resumen Ejecutivo
[2-3 frases sobre el estado operativo actual de la flota]

## Insights Clave
[5-7 observaciones relevantes basadas en los datos, con viñetas]

## Alertas y Puntos de Atención
[Anomalías, riesgos operacionales o puntos críticos. Si no hay alertas, indícalo.]

## Recomendaciones Accionables
[3-5 acciones concretas para el responsable de flota]

Sé directo, usa terminología profesional de gestión de flotas y enfócate en valor de negocio. Responde en español.

Datos actuales de la flota:
${JSON.stringify(summary, null, 2)}`;

  try {
    const client  = new Anthropic({ apiKey: anthropicApiKey });
    const message = await client.messages.create({
      model:      'claude-opus-4-6',
      max_tokens: 1500,
      messages:   [{ role: 'user', content: prompt }],
    });
    res.json({ insights: message.content[0].text });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`\n✅  Webfleet Bulk Tool corriendo en → http://localhost:${PORT}\n`);
});
