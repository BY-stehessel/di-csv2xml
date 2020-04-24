use csv;
use std::io;

pub const CUSTOMER_EXTENSION_PREFIX: &str = "CUEX_";

/// Holds all the state we need to parse the input csv line by line.
pub struct CsvSource<R> {
    reader: csv::Reader<R>,
}

impl<R: io::Read> CsvSource<R> {
    /// Creates a new `CsvSource` from any Read and a delimiter.
    pub fn new(input: R, delimiter: u8) -> io::Result<(Self, Schema)> {
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(delimiter as u8)
            .from_reader(input);
        let header = reader.headers()?.clone();
        let (extensions, standard): (Vec<_>, Vec<_>) = (0..header.len())
            .partition(|&index| header[index].starts_with(CUSTOMER_EXTENSION_PREFIX));
        let schema = Schema {
            header,
            standard,
            extensions,
        };

        let source = CsvSource {
            reader,
        };

        Ok((source, schema))
    }

    /// Return the next Record of the csv or `None`.
    pub fn read_record(&mut self, record: &mut Record) -> io::Result<bool> {
        Ok(if self.reader.read_record(&mut record.values)? {
            true
        } else {
            false
        })
    }
}

pub struct Schema {
    /// Indices of standard records within csv.
    standard: Vec<usize>,
    /// Indices of extension records within csv. Columns with a column name starting with `CUEX_`
    /// are considered non standard extensions. These will show up within the `<CustomerExtensions>`
    /// tag within the XML.
    extensions: Vec<usize>,
    /// Header of the csv file containing the names of the columns.
    header: csv::StringRecord,
}

/// Represents one data record (i.e. a single line of csv to be converted into an XML tag).  
pub struct Record<'s> {
    pub schema: &'s Schema,
    /// Current record which has been parsed from csv and is to be written as XML.
    pub values: csv::StringRecord,
}

impl Schema {
    /// Returns an iterator over all standard tags. `Item = (tag_name, value)`
    pub fn standard(&self) -> impl Iterator<Item = (&str, &str)> {
        self.field_it(&self.standard, 0, values)
    }

    /// Returns an iterator over all customer extension tags. `Item = (tag_name, value)`
    pub fn extensions(&self) -> impl Iterator<Item = (&str, &str)> {
        // This helps us to cut of the leading 'CUEX_' prefix from tag names
        let skip = CUSTOMER_EXTENSION_PREFIX.len();
        self.field_it(&self.extensions, skip, values)
    }

    fn field_it(& self, fields: & [usize], skip: usize, values: & csv::StringRecord) -> impl Iterator<Item = (&str, &str)> {
        fields
            .iter()
            .map(move |&index| (&self.header[index][skip..], &values[index]))
            // Empty strings are treated as null and will not be rendered in XML
            .filter(|&(_, ref v)| !v.is_empty())
    }
}
